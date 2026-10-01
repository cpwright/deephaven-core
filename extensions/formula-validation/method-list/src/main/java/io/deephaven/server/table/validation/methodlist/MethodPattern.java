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
import java.util.ArrayList;
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
    private static final Pattern TYPE_PATTERN =
            Pattern.compile("[\\p{javaJavaIdentifierPart}*]+(?:\\.\\.?[\\p{javaJavaIdentifierPart}*]+)*");
    private static final Pattern NAME_PATTERN = Pattern.compile("[\\p{javaJavaIdentifierPart}*]+");
    private static final Pattern ARRAY_SUFFIX = Pattern.compile("(?:\\s*\\[\\s*])*");

    private final String pattern;
    private final String declaringType;
    private final String expression;
    private final ClassLoader defaultLoader = MethodPattern.class.getClassLoader();
    /**
     * Parsed for {@link #defaultLoader}. AspectJ does not document matching as thread safe, so matches against this
     * expression are synchronized on it.
     */
    private final PointcutExpression defaultExpression;

    /**
     * Parse a pattern.
     *
     * @param pattern the pattern text
     * @throws IllegalArgumentException if the pattern is malformed
     */
    MethodPattern(final String pattern) {
        this.pattern = pattern;
        declaringType = pattern.trim().substring(0, Math.max(0, pattern.trim().indexOf(' ')));
        expression = toAspectJ(pattern);
        defaultExpression = parse(expression, defaultLoader);
    }

    /**
     * Does this pattern match the given constructor?
     *
     * @param constructor the constructor to test
     * @return true if the constructor matches
     */
    boolean matches(final Constructor<?> constructor) {
        return matches(constructor, expression,
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
        if (!matches(method, expression, (pe, member) -> pe.matchesMethodExecution((Method) member))) {
            return false;
        }
        final int modifiers = method.getModifiers();
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers)
                || !hasSuperclassInAnotherRuntimePackage(method.getDeclaringClass())) {
            return true;
        }
        // AspectJ compares package names, but a package-private method only overrides within its runtime package,
        // which is also scoped by class loader; require the declaring class itself to match
        return matches(method, "(" + expression + ") && within(" + declaringType + ")",
                (pe, member) -> pe.matchesMethodExecution((Method) member));
    }

    private static boolean hasSuperclassInAnotherRuntimePackage(final Class<?> type) {
        for (Class<?> superclass = type.getSuperclass(); superclass != null; superclass =
                superclass.getSuperclass()) {
            if (superclass.getPackageName().equals(type.getPackageName())
                    && superclass.getClassLoader() != type.getClassLoader()) {
                return true;
            }
        }
        return false;
    }

    private interface Matcher {
        ShadowMatch match(PointcutExpression expression, Executable member);
    }

    private boolean matches(final Executable member, final String aspectJExpression, final Matcher matcher) {
        try {
            if (!aspectJExpression.equals(expression)) {
                final ClassLoader loader = member.getDeclaringClass().getClassLoader();
                return matcher.match(parse(aspectJExpression, loader == null ? defaultLoader : loader), member)
                        .alwaysMatches();
            }
            if (isVisibleByName(member.getDeclaringClass())) {
                synchronized (defaultExpression) {
                    return matcher.match(defaultExpression, member).alwaysMatches();
                }
            }
            final ClassLoader loader = member.getDeclaringClass().getClassLoader();
            return matcher.match(parse(expression, loader == null ? defaultLoader : loader), member).alwaysMatches();
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
