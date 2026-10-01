//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.lang.reflect.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A single <code>#declaring class# #method name#(#argument list#)</code> pattern, matched against reflective
 * {@link Method methods} and {@link Constructor constructors}. The syntax is described by
 * {@link MethodListInvocationValidator}.
 */
final class MethodPattern {
    private static final String CONSTRUCTOR_NAME = "<constructor>";
    private static final String ANY_ARGUMENTS = "..";
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[\\p{javaJavaIdentifierPart}*]+");
    private static final Set<String> PRIMITIVE_NAMES =
            Set.of("boolean", "byte", "char", "short", "int", "long", "float", "double", "void");

    private final Pattern declaringType;
    private final Pattern name;
    /**
     * One entry per element of the argument list: a {@link Pattern} for a single argument, or {@link #ANY_ARGUMENTS}
     * for zero or more arguments of any type.
     */
    private final List<Object> arguments;

    /**
     * Parse a pattern.
     *
     * @param pattern the pattern text
     * @throws IllegalArgumentException if the pattern is malformed
     */
    MethodPattern(final String pattern) {
        final String trimmed = pattern.trim();
        final int space = trimmed.indexOf(' ');
        final int open = trimmed.indexOf('(');
        if (space <= 0 || open < space || !trimmed.endsWith(")")) {
            throw new IllegalArgumentException(
                    "Expected '<declaring class> <method name>(<argument list>)', but got '" + pattern + "'");
        }
        final String declaringTypeText = trimmed.substring(0, space);
        if (declaringTypeText.equals("*")) {
            throw new IllegalArgumentException("Use '*..*' rather than '*' to match every declaring class: '"
                    + pattern + "'");
        }
        declaringType = typePattern(declaringTypeText, false);
        name = namePattern(trimmed.substring(space + 1, open).trim());

        final String argumentList = trimmed.substring(open + 1, trimmed.length() - 1).trim();
        final List<Object> parsedArguments = new ArrayList<>();
        if (!argumentList.isEmpty()) {
            final String[] split = argumentList.split(",", -1);
            for (int ai = 0; ai < split.length; ++ai) {
                final String argument = split[ai].trim();
                if (argument.equals(ANY_ARGUMENTS)) {
                    parsedArguments.add(ANY_ARGUMENTS);
                } else if (argument.endsWith("...")) {
                    if (ai != split.length - 1) {
                        throw new IllegalArgumentException("Only the last argument may be variable arity: '"
                                + pattern + "'");
                    }
                    parsedArguments.add(typePattern(argument.substring(0, argument.length() - 3) + "[]", true));
                } else {
                    parsedArguments.add(typePattern(argument, true));
                }
            }
        }
        arguments = List.copyOf(parsedArguments);
    }

    /**
     * Does this pattern match the given constructor?
     *
     * @param constructor the constructor to test
     * @return true if the constructor's declaring class, the name {@code <constructor>} and the parameter types all
     *         match
     */
    boolean matches(final Constructor<?> constructor) {
        return name.matcher(CONSTRUCTOR_NAME).matches()
                && typeMatches(declaringType, constructor.getDeclaringClass())
                && argumentsMatch(constructor.getParameterTypes());
    }

    /**
     * Does this pattern match the given method?
     *
     * <p>
     * A method matches when its name and parameter types match and either its declaring class matches, or it is an
     * instance method that overrides a method declared by a class or interface that matches. For an override of a
     * generic method, the argument list may match either the overriding or the overridden parameter types.
     * </p>
     *
     * @param method the method to test
     * @return true if the method matches
     */
    boolean matches(final Method method) {
        if (!name.matcher(method.getName()).matches()) {
            return false;
        }
        final Class<?> declaringClass = method.getDeclaringClass();
        if (typeMatches(declaringType, declaringClass) && argumentsMatch(method.getParameterTypes())) {
            return true;
        }
        if (Modifier.isStatic(method.getModifiers()) || Modifier.isPrivate(method.getModifiers())) {
            return false;
        }

        final Deque<Class<?>> pending = new ArrayDeque<>(directSupertypes(declaringClass));
        final Set<Class<?>> visited = new HashSet<>();
        while (!pending.isEmpty()) {
            final Class<?> supertype = pending.pop();
            if (!visited.add(supertype)) {
                continue;
            }
            if (typeMatches(declaringType, supertype)) {
                for (final Method candidate : supertype.getDeclaredMethods()) {
                    if (isOverriddenBy(candidate, method)
                            && (argumentsMatch(method.getParameterTypes())
                                    || argumentsMatch(candidate.getParameterTypes()))) {
                        return true;
                    }
                }
            }
            pending.addAll(directSupertypes(supertype));
        }
        return false;
    }

    private static List<Class<?>> directSupertypes(final Class<?> type) {
        final List<Class<?>> result = new ArrayList<>(Arrays.asList(type.getInterfaces()));
        if (type.getSuperclass() != null) {
            result.add(type.getSuperclass());
        } else if (type.isInterface()) {
            // an interface implicitly declares the public methods of Object
            result.add(Object.class);
        }
        return result;
    }

    /**
     * Is {@code candidate}, declared by a supertype of {@code method}'s declaring class, overridden by {@code method}?
     * Type variables in the candidate's parameters are resolved through the generic supertypes of the overriding class,
     * so that {@code Integer.compareTo(Integer)} overrides {@code Comparable.compareTo(T)}.
     */
    private static boolean isOverriddenBy(final Method candidate, final Method method) {
        final int modifiers = candidate.getModifiers();
        if (!candidate.getName().equals(method.getName())
                || candidate.getParameterCount() != method.getParameterCount()
                || Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers) || candidate.isBridge()) {
            return false;
        }
        if (!Modifier.isPublic(modifiers) && !Modifier.isProtected(modifiers)
                && !samePackage(candidate.getDeclaringClass(), method.getDeclaringClass())) {
            // a package-private method is only overridden within its own runtime package
            return false;
        }
        final Map<TypeVariable<?>, Type> bindings = new HashMap<>();
        bindTypeArguments(method.getDeclaringClass(), bindings, new HashSet<>());
        if (!Arrays.equals(candidate.getParameterTypes(), method.getParameterTypes())) {
            final Type[] candidateParameters = candidate.getGenericParameterTypes();
            final Class<?>[] methodParameters = method.getParameterTypes();
            for (int pi = 0; pi < methodParameters.length; ++pi) {
                if (erase(candidateParameters[pi], bindings) != methodParameters[pi]) {
                    return false;
                }
            }
        }
        // generated bytecode may declare a same-signature method whose return type is not substitutable
        final Class<?> candidateReturn = erase(candidate.getGenericReturnType(), bindings);
        final Class<?> methodReturn = method.getReturnType();
        if (candidateReturn.isPrimitive() || methodReturn.isPrimitive()) {
            return candidateReturn == methodReturn;
        }
        return candidateReturn.isAssignableFrom(methodReturn);
    }

    private static boolean samePackage(final Class<?> first, final Class<?> second) {
        return first.getClassLoader() == second.getClassLoader()
                && first.getPackageName().equals(second.getPackageName());
    }

    private static void bindTypeArguments(final Class<?> type, final Map<TypeVariable<?>, Type> bindings,
            final Set<Class<?>> visited) {
        if (type == null || !visited.add(type)) {
            return;
        }
        final List<Type> genericSupertypes = new ArrayList<>(Arrays.asList(type.getGenericInterfaces()));
        if (type.getGenericSuperclass() != null) {
            genericSupertypes.add(type.getGenericSuperclass());
        }
        for (final Type supertype : genericSupertypes) {
            if (supertype instanceof ParameterizedType) {
                final ParameterizedType parameterized = (ParameterizedType) supertype;
                final TypeVariable<?>[] variables = ((Class<?>) parameterized.getRawType()).getTypeParameters();
                final Type[] typeArguments = parameterized.getActualTypeArguments();
                for (int vi = 0; vi < variables.length; ++vi) {
                    bindings.putIfAbsent(variables[vi], typeArguments[vi]);
                }
                bindTypeArguments((Class<?>) parameterized.getRawType(), bindings, visited);
            } else if (supertype instanceof Class) {
                bindTypeArguments((Class<?>) supertype, bindings, visited);
            }
        }
    }

    private static Class<?> erase(final Type type, final Map<TypeVariable<?>, Type> bindings) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        if (type instanceof GenericArrayType) {
            return Array.newInstance(erase(((GenericArrayType) type).getGenericComponentType(), bindings), 0)
                    .getClass();
        }
        if (type instanceof TypeVariable) {
            final Type bound = bindings.get(type);
            if (bound != null && bound != type) {
                return erase(bound, bindings);
            }
            return erase(((TypeVariable<?>) type).getBounds()[0], bindings);
        }
        if (type instanceof WildcardType) {
            return erase(((WildcardType) type).getUpperBounds()[0], bindings);
        }
        return Object.class;
    }

    private boolean argumentsMatch(final Class<?>[] parameterTypes) {
        return argumentsMatch(0, parameterTypes, 0);
    }

    private boolean argumentsMatch(final int argumentIndex, final Class<?>[] parameterTypes,
            final int parameterIndex) {
        if (argumentIndex == arguments.size()) {
            return parameterIndex == parameterTypes.length;
        }
        final Object argument = arguments.get(argumentIndex);
        if (argument == ANY_ARGUMENTS) {
            for (int pi = parameterIndex; pi <= parameterTypes.length; ++pi) {
                if (argumentsMatch(argumentIndex + 1, parameterTypes, pi)) {
                    return true;
                }
            }
            return false;
        }
        return parameterIndex < parameterTypes.length
                && typeMatches((Pattern) argument, parameterTypes[parameterIndex])
                && argumentsMatch(argumentIndex + 1, parameterTypes, parameterIndex + 1);
    }

    private static boolean typeMatches(final Pattern pattern, final Class<?> type) {
        final String canonicalName = type.getCanonicalName();
        if (canonicalName != null && pattern.matcher(canonicalName).matches()) {
            return true;
        }
        // local and anonymous classes have no canonical name; "$" in a pattern also matches a nested class
        return !type.isArray() && pattern.matcher(type.getName()).matches();
    }

    /**
     * Translate a type pattern to a regular expression over canonical or binary class names. A {@code *} alone matches
     * any argument type; otherwise it matches within one segment of a name. {@code ..} matches any number of
     * intermediate packages, and an unqualified name that is not a primitive matches only the class of that name in
     * {@code java.lang}.
     */
    private static Pattern typePattern(final String text, final boolean argument) {
        String element = text.trim();
        final StringBuilder arraySuffix = new StringBuilder();
        while (argument && element.endsWith("[]")) {
            element = element.substring(0, element.length() - 2).trim();
            arraySuffix.append("\\[\\]");
        }
        if (element.isEmpty() || element.startsWith(".") || element.endsWith(".") || element.contains("...")) {
            throw new IllegalArgumentException("Invalid type pattern: '" + text + "'");
        }
        if ((argument && element.equals("*")) || element.equals("*..*")) {
            return Pattern.compile(".*" + arraySuffix);
        }

        final StringBuilder regex = new StringBuilder();
        int start = 0;
        while (start < element.length()) {
            final int dot = element.indexOf('.', start);
            final int end = dot < 0 ? element.length() : dot;
            final String segment = element.substring(start, end);
            if (!IDENTIFIER_PATTERN.matcher(segment).matches()) {
                throw new IllegalArgumentException("Invalid type pattern: '" + text + "'");
            }
            regex.append(globToRegex(segment, "[^.]*"));
            if (dot < 0) {
                break;
            }
            if (element.startsWith("..", dot)) {
                regex.append("\\.(?:.*\\.)?");
                start = dot + 2;
            } else {
                regex.append("\\.");
                start = dot + 1;
            }
        }

        String result = regex.toString();
        if (element.indexOf('.') < 0 && element.indexOf('*') < 0 && !PRIMITIVE_NAMES.contains(element)) {
            result = "java\\.lang\\." + result;
        }
        return Pattern.compile(result + arraySuffix);
    }

    private static Pattern namePattern(final String text) {
        if (text.equals(CONSTRUCTOR_NAME)) {
            return Pattern.compile(Pattern.quote(CONSTRUCTOR_NAME));
        }
        if (!IDENTIFIER_PATTERN.matcher(text).matches()) {
            throw new IllegalArgumentException("Invalid method name pattern: '" + text + "'");
        }
        // a name of only wildcards also matches constructors
        return Pattern.compile(text.replace("*", "").isEmpty() ? ".*" : globToRegex(text, "[^<]*"));
    }

    private static String globToRegex(final String glob, final String wildcard) {
        final StringBuilder regex = new StringBuilder();
        int start = 0;
        for (int star = glob.indexOf('*'); star >= 0; star = glob.indexOf('*', start)) {
            regex.append(literal(glob.substring(start, star))).append(wildcard);
            start = star + 1;
        }
        return regex.append(literal(glob.substring(start))).toString();
    }

    private static String literal(final String text) {
        // "$" separates a nested class from its enclosing class in a binary name, and "." in a canonical name
        return text.isEmpty() ? "" : Pattern.quote(text).replace("$", "\\E[.$]\\Q");
    }
}
