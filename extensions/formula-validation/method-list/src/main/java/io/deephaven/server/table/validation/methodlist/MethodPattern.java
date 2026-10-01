//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import org.aspectj.weaver.tools.PointcutExpression;
import org.aspectj.weaver.tools.PointcutParser;
import org.aspectj.weaver.tools.PointcutPrimitive;
import org.aspectj.weaver.tools.ShadowMatch;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A single <code>#declaring class# #method name#(#argument list#)</code> pattern, translated to an AspectJ
 * {@code execution} pointcut and matched against reflective {@link Method methods} and {@link Constructor
 * constructors}. The syntax is described by {@link MethodListInvocationValidator}.
 *
 * <p>
 * AspectJ resolves the types named by a pointcut through a single class loader, and reports a class that loader cannot
 * find as an error. A member of a class that is not visible by name from this class's loader is therefore matched by an
 * expression parsed for the member's own loader. That expression is not retained, as it would keep the loader
 * reachable.
 * </p>
 */
final class MethodPattern {
    private static final String CONSTRUCTOR_NAME = "<constructor>";
    private static final String ANY_ARGUMENTS = "..";
    private static final int EXPRESSION = 0;
    private static final int WITHIN = 1;
    private static final int ANCESTOR = 2;
    private static final Pattern TYPE_PATTERN =
            Pattern.compile("[\\p{javaJavaIdentifierPart}*]+(?:\\.\\.?[\\p{javaJavaIdentifierPart}*]+)*");
    private static final Pattern NAME_PATTERN = Pattern.compile("[\\p{javaJavaIdentifierPart}*]+");
    private static final Pattern ARRAY_SUFFIX = Pattern.compile("(?:\\s*\\[\\s*])*");

    private final String pattern;
    private final String declaringType;
    /**
     * The AspectJ expressions this pattern is matched with: {@link #EXPRESSION} is the whole pattern, {@link #WITHIN}
     * restricts it to members of a class that itself matches the declaring class pattern, and {@link #ANCESTOR} matches
     * only the declaring class and method name of such members.
     */
    private final String[] expressions = new String[3];
    private final ClassLoader defaultLoader = MethodPattern.class.getClassLoader();
    /**
     * {@link #expressions} parsed for {@link #defaultLoader}. AspectJ does not document matching as thread safe, so
     * matches against each are synchronized on it.
     */
    private final PointcutExpression[] defaultExpressions = new PointcutExpression[3];

    /**
     * Parse a pattern.
     *
     * @param pattern the pattern text
     * @throws IllegalArgumentException if the pattern is malformed
     */
    MethodPattern(final String pattern) {
        this.pattern = pattern;
        declaringType = pattern.trim().substring(0, Math.max(0, pattern.trim().indexOf(' ')));
        expressions[EXPRESSION] = toAspectJ(pattern);
        expressions[WITHIN] = "(" + expressions[EXPRESSION] + ") && within(" + declaringType + ")";
        final String trimmed = pattern.trim();
        final String name = trimmed.substring(trimmed.indexOf(' ') + 1, trimmed.indexOf('(')).trim();
        expressions[ANCESTOR] = name.equals(CONSTRUCTOR_NAME) ? expressions[WITHIN]
                : "execution(* " + declaringType + "." + name + "(..)) && within(" + declaringType + ")";
        for (int ei = 0; ei < expressions.length; ++ei) {
            defaultExpressions[ei] = parse(expressions[ei], defaultLoader);
        }
    }

    /**
     * Does this pattern match the given constructor?
     *
     * @param constructor the constructor to test
     * @return true if the constructor matches
     */
    boolean matches(final Constructor<?> constructor) {
        return matches(constructor, EXPRESSION,
                (pe, member) -> pe.matchesConstructorExecution((Constructor<?>) member));
    }

    /**
     * Does this pattern match the given method?
     *
     * <p>
     * A method matches when its name and parameter types match and either its declaring class matches, or it is an
     * instance method that overrides a method declared by a class or interface that matches.
     * </p>
     *
     * @param method the method to test
     * @return true if the method matches
     */
    boolean matches(final Method method) {
        if (!matches(method, EXPRESSION, METHOD_MATCHER)) {
            return false;
        }
        if (matches(method, WITHIN, METHOD_MATCHER)) {
            return true;
        }
        // AspectJ matched through a supertype, but it treats a package-private method as inherited even outside its
        // runtime package, which is scoped by class loader as well as name; require a matching supertype to declare a
        // method with this name that this one can override; AspectJ has already matched the argument list
        final Class<?> declaringClass = method.getDeclaringClass();
        final Deque<Class<?>> pending = new ArrayDeque<>(directSupertypes(declaringClass));
        final Set<Class<?>> visited = new HashSet<>();
        while (!pending.isEmpty()) {
            final Class<?> supertype = pending.pop();
            if (!visited.add(supertype)) {
                continue;
            }
            for (final Method candidate : supertype.getDeclaredMethods()) {
                if (candidate.getName().equals(method.getName())
                        && candidate.getParameterCount() == method.getParameterCount()
                        && canBeOverriddenFrom(candidate, declaringClass)
                        && matches(candidate, ANCESTOR, METHOD_MATCHER)) {
                    return true;
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

    private static boolean canBeOverriddenFrom(final Method candidate, final Class<?> overridingClass) {
        final int modifiers = candidate.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isPrivate(modifiers) || candidate.isBridge()) {
            return false;
        }
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers)) {
            return true;
        }
        final Class<?> candidateClass = candidate.getDeclaringClass();
        return candidateClass.getClassLoader() == overridingClass.getClassLoader()
                && candidateClass.getPackageName().equals(overridingClass.getPackageName());
    }

    private interface Matcher {
        ShadowMatch match(PointcutExpression expression, Executable member);
    }

    private static final Matcher METHOD_MATCHER = (pe, member) -> pe.matchesMethodExecution((Method) member);

    private boolean matches(final Executable member, final int expressionIndex, final Matcher matcher) {
        try {
            if (isVisibleByName(member.getDeclaringClass())) {
                final PointcutExpression parsed = defaultExpressions[expressionIndex];
                synchronized (parsed) {
                    return matcher.match(parsed, member).alwaysMatches();
                }
            }
            final ClassLoader loader = member.getDeclaringClass().getClassLoader();
            return matcher.match(parse(expressions[expressionIndex], loader == null ? defaultLoader : loader),
                    member).alwaysMatches();
        } catch (RuntimeException e) {
            // AspectJ reports a type it cannot resolve as an exception; a member we cannot reason about is not
            // permitted
            return false;
        }
    }

    private boolean isVisibleByName(final Class<?> type) {
        try {
            return Class.forName(type.getName(), false, defaultLoader) == type;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    private PointcutExpression parse(final String aspectJExpression, final ClassLoader loader) {
        final PointcutParser parser = PointcutParser
                .getPointcutParserSupportingSpecifiedPrimitivesAndUsingSpecifiedClassLoaderForResolution(
                        Set.of(PointcutPrimitive.EXECUTION, PointcutPrimitive.WITHIN), loader);
        final Properties lint = new Properties();
        // a pattern may name a class that is not present, which matches nothing
        lint.setProperty("invalidAbsoluteTypeName", "ignore");
        // "T[]" and "T..." are equivalent here; both alternatives are part of the expression
        lint.setProperty("cantMatchArrayTypeOnVarargs", "ignore");
        parser.setLintProperties(lint);
        try {
            return parser.parsePointcutExpression(aspectJExpression);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Could not translate method pattern '" + pattern + "' to AspectJ", e);
        }
    }

    /**
     * Translate our pattern to an AspectJ pointcut, e.g. {@code java.lang.String valueOf(char[])} becomes
     * {@code execution(* java.lang.String.valueOf(char[])) || execution(* java.lang.String.valueOf(char...))}. Each
     * element is validated first, so that a pattern cannot contain other pointcut syntax.
     */
    private static String toAspectJ(final String pattern) {
        final String trimmed = pattern.trim();
        final int space = trimmed.indexOf(' ');
        final int open = trimmed.indexOf('(');
        if (space <= 0 || open < space || !trimmed.endsWith(")")) {
            throw new IllegalArgumentException(
                    "Expected '<declaring class> <method name>(<argument list>)', but got '" + pattern + "'");
        }
        final String declaringType = trimmed.substring(0, space);
        if (declaringType.equals("*")) {
            throw new IllegalArgumentException("Use '*..*' rather than '*' to match every declaring class: '"
                    + pattern + "'");
        }
        if (!TYPE_PATTERN.matcher(declaringType).matches()) {
            throw new IllegalArgumentException("Invalid type pattern: '" + declaringType + "'");
        }
        final String name = trimmed.substring(space + 1, open).trim();
        final boolean constructor = name.equals(CONSTRUCTOR_NAME);
        if (!constructor && !NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid method name pattern: '" + name + "'");
        }

        final List<String> arguments = new ArrayList<>();
        final String argumentList = trimmed.substring(open + 1, trimmed.length() - 1).trim();
        if (!argumentList.isEmpty()) {
            final String[] split = argumentList.split(",", -1);
            for (int ai = 0; ai < split.length; ++ai) {
                String argument = split[ai].trim();
                if (argument.equals(ANY_ARGUMENTS)) {
                    arguments.add(ANY_ARGUMENTS);
                    continue;
                }
                if (argument.endsWith("...")) {
                    if (ai != split.length - 1) {
                        throw new IllegalArgumentException(
                                "Only the last argument may be variable arity: '" + pattern + "'");
                    }
                    argument = argument.substring(0, argument.length() - 3) + "[]";
                }
                final int bracket = argument.indexOf('[');
                final String element = bracket < 0 ? argument : argument.substring(0, bracket).trim();
                if (!TYPE_PATTERN.matcher(element).matches()
                        || !ARRAY_SUFFIX.matcher(bracket < 0 ? "" : argument.substring(bracket)).matches()) {
                    throw new IllegalArgumentException("Invalid type pattern: '" + argument + "'");
                }
                arguments.add(element + "[]".repeat(bracket < 0 ? 0 : countArrayDimensions(argument)));
            }
        }

        final List<String> argumentLists = new ArrayList<>();
        argumentLists.add(String.join(", ", arguments));
        if (!arguments.isEmpty() && arguments.get(arguments.size() - 1).endsWith("[]")) {
            // AspectJ distinguishes a varargs parameter from an array parameter, but our patterns do not
            final List<String> varargs = new ArrayList<>(arguments);
            final String last = varargs.remove(varargs.size() - 1);
            varargs.add(last.substring(0, last.length() - 2) + "...");
            argumentLists.add(String.join(", ", varargs));
        }

        final List<String> alternatives = new ArrayList<>();
        // a method name of only wildcards also matches constructors
        final boolean matchesConstructors = constructor || name.replace("*", "").isEmpty();
        for (final String argumentText : argumentLists) {
            if (!constructor) {
                alternatives.add("execution(* " + declaringType + "." + name + "(" + argumentText + "))");
            }
            if (matchesConstructors) {
                alternatives.add("execution(" + declaringType + ".new(" + argumentText + "))");
            }
        }
        return String.join(" || ", alternatives);
    }

    private static int countArrayDimensions(final String argument) {
        int dimensions = 0;
        for (int ci = 0; ci < argument.length(); ++ci) {
            if (argument.charAt(ci) == '[') {
                ++dimensions;
            }
        }
        return dimensions;
    }
}
