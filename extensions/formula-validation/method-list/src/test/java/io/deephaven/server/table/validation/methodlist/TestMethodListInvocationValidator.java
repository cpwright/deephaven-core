//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.server.table.validation.methodlist.other.OtherPackageSub;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestMethodListInvocationValidator {
    @Test
    public void testDeclaringClass() throws NoSuchMethodException {
        assertPermitted("java.lang.Math abs(..)", Math.class.getMethod("abs", double.class));
        assertPermitted("java.time.Instant *(..)", Instant.class.getMethod("parse", CharSequence.class));
        assertPermitted("java.lang.Integer *(int)", Integer.class.getMethod("valueOf", int.class));
        assertNotPermitted("java.lang.Integer *(int)", Integer.class.getMethod("valueOf", String.class));
        assertNotPermitted("java.lang.Long *(..)", Integer.class.getMethod("valueOf", int.class));
        assertPermitted("java.lang.String to*Case()", String.class.getMethod("toUpperCase"));
        assertNotPermitted("java.lang.String to*Case()", String.class.getMethod("toString"));
        assertPermitted("java.lang.Ma* max(..)", Math.class.getMethod("max", int.class, int.class));
    }

    @Test
    public void testPackageWildcards() throws NoSuchMethodException {
        assertPermitted("*..* *(..)", Math.class.getMethod("abs", int.class));
        assertPermitted("java.lang.* *(..)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java..* *(..)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.util.* *(..)", ArrayList.class.getMethod("trimToSize"));
        // "*" does not cross a package boundary, ".." does
        assertNotPermitted("java.util.* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
        assertPermitted("java.util..* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
        assertNotPermitted("javax..* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
    }

    @Test
    public void testNestedClasses() throws NoSuchMethodException {
        assertPermitted("java.util.Map.Entry getKey()", Map.Entry.class.getMethod("getKey"));
        assertPermitted("java.util.Map$Entry getKey()", Map.Entry.class.getMethod("getKey"));
        assertNotPermitted("java.util.Entry getKey()", Map.Entry.class.getMethod("getKey"));
    }

    @Test
    public void testArguments() throws NoSuchMethodException {
        assertPermitted("java.lang.String valueOf(char[])", String.class.getMethod("valueOf", char[].class));
        assertNotPermitted("java.lang.String valueOf(java.lang.Object[])",
                String.class.getMethod("valueOf", char[].class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", int.class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", Object.class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", char[].class));
        assertPermitted("java.lang.Math max(*, *)", Math.class.getMethod("max", int.class, int.class));
        assertNotPermitted("java.lang.Math max(*)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.lang.Math max(int, int)", Math.class.getMethod("max", int.class, int.class));
        assertNotPermitted("java.lang.Math max(long, long)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.lang.String *(int, ..)", String.class.getMethod("substring", int.class, int.class));
        assertPermitted("java.lang.String *(int, ..)", String.class.getMethod("substring", int.class));
        assertNotPermitted("java.lang.String *(int, ..)", String.class.getMethod("indexOf", String.class, int.class));
        assertPermitted("java.lang.String *(.., int)", String.class.getMethod("indexOf", String.class, int.class));
        assertPermitted("java.util.Arrays toString(int[])", Arrays.class.getMethod("toString", int[].class));
        assertNotPermitted("java.util.Arrays toString(int[])", Arrays.class.getMethod("toString", long[].class));
        assertPermitted("java.util.Arrays deepToString(java.lang.Object[])",
                Arrays.class.getMethod("deepToString", Object[].class));
    }

    @Test
    public void testVarargs() throws NoSuchMethodException {
        final Method format = String.class.getMethod("format", String.class, Object[].class);
        assertPermitted("java.lang.String format(java.lang.String, java.lang.Object[])", format);
        assertPermitted("java.lang.String format(java.lang.String, java.lang.Object...)", format);
        assertPermitted("java.util.Arrays asList(Object[])", Arrays.class.getMethod("asList", Object[].class));
        assertPermitted("java.util.Arrays asList(Object...)", Arrays.class.getMethod("asList", Object[].class));
    }

    @Test
    public void testUnqualifiedTypes() throws NoSuchMethodException {
        // only java.lang types may be unqualified
        assertPermitted("java.lang.String valueOf(Object)", String.class.getMethod("valueOf", Object.class));
        assertPermitted("String length()", String.class.getMethod("length"));
        assertNotPermitted("Collections emptyMap()", Collections.class.getMethod("emptyMap"));
        assertNotPermitted("java.util.Collections unmodifiableMap(Map)",
                Collections.class.getMethod("unmodifiableMap", Map.class));
        assertPermitted("java.util.Collections unmodifiableMap(java.util.Map)",
                Collections.class.getMethod("unmodifiableMap", Map.class));
    }

    @Test
    public void testMissingTypes() throws NoSuchMethodException {
        assertNotPermitted("com.example.Missing length()", String.class.getMethod("length"));
        assertNotPermitted("java.lang.String *(com.example.Missing)", String.class.getMethod("length"));
        assertNotPermitted("java.lang.String *(com.example.Missing)",
                String.class.getMethod("valueOf", Object.class));
    }

    @Test
    public void testObjectOverrides() throws NoSuchMethodException {
        assertPermitted("java.lang.Object toString()", Object.class.getMethod("toString"));
        assertPermitted("java.lang.Object toString()", Integer.class.getMethod("toString"));
        assertPermitted("java.lang.Object toString()", StringBuilder.class.getMethod("toString"));
        assertPermitted("java.lang.Object hashCode()", Integer.class.getMethod("hashCode"));
        assertPermitted("java.lang.Object equals(java.lang.Object)", String.class.getMethod("equals", Object.class));
        assertPermitted("java.lang.Object getClass()", Integer.class.getMethod("getClass"));
        // static methods do not override
        assertNotPermitted("java.lang.Object hashCode()", Integer.class.getMethod("hashCode", int.class));
        assertNotPermitted("java.lang.Object toString(..)", Integer.class.getMethod("toString", int.class));
        assertNotPermitted("java.lang.Object *(..)", Integer.class.getMethod("valueOf", int.class));
        // an override is matched by its supertype, not the other way around
        assertNotPermitted("java.lang.String toString()", Object.class.getMethod("toString"));
    }

    @Test
    public void testOverrides() throws NoSuchMethodException {
        assertPermitted("java.lang.Number *(..)", Number.class.getMethod("intValue"));
        assertPermitted("java.lang.Number *(..)", Integer.class.getMethod("intValue"));
        assertPermitted("java.lang.Number intValue()", BigDecimal.class.getMethod("intValue"));
        assertNotPermitted("java.lang.Number *(..)", Integer.class.getMethod("valueOf", int.class));
        assertNotPermitted("java.lang.Number *(..)", Integer.class.getMethod("compareTo", Integer.class));
        assertPermitted("java.lang.CharSequence length()", String.class.getMethod("length"));
        assertPermitted("java.util.List size()", ArrayList.class.getMethod("size"));
        assertPermitted("java.util.Collection size()", ArrayList.class.getMethod("size"));
        assertNotPermitted("java.util.List size()", ConcurrentHashMap.class.getMethod("size"));
        assertNotPermitted("java.util.List trimToSize()", ArrayList.class.getMethod("trimToSize"));
        // a covariant return type still overrides
        assertPermitted("java.lang.Appendable append(java.lang.CharSequence)",
                StringBuilder.class.getMethod("append", CharSequence.class));
    }

    @Test
    public void testGenericOverrides() throws NoSuchMethodException {
        final Method compareTo = Integer.class.getMethod("compareTo", Integer.class);
        assertPermitted("java.lang.Comparable compareTo(..)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(java.lang.Object)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(java.lang.Integer)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(*)", compareTo);
        assertNotPermitted("java.lang.Comparable compareTo(java.lang.String)", compareTo);
        assertNotPermitted("java.lang.Integer compareTo(java.lang.Object)", compareTo);
        assertPermitted("java.lang.Integer compareTo(java.lang.Integer)", compareTo);
        assertPermitted("java.util.List add(java.lang.Object)", ArrayList.class.getMethod("add", Object.class));
    }

    @Test
    public void testConstructors() throws NoSuchMethodException {
        assertPermitted("java.lang.Integer <constructor>(int)", Integer.class.getConstructor(int.class));
        assertNotPermitted("java.lang.Integer <constructor>(int)", Integer.class.getConstructor(String.class));
        assertPermitted("java.lang.Integer <constructor>(..)", Integer.class.getConstructor(String.class));
        assertPermitted("java.math.BigInteger <constructor>(String)", BigInteger.class.getConstructor(String.class));
        assertNotPermitted("java.math.BigInteger <constructor>(String)", BigDecimal.class.getConstructor(String.class));
        assertNotPermitted("java.lang.Number <constructor>()", Integer.class.getConstructor(int.class));
        // a method name of only wildcards matches constructors too
        assertPermitted("java.math.BigDecimal *(..)", BigDecimal.class.getConstructor(String.class));
        assertPermitted("java.lang.String *(char[])", String.class.getConstructor(char[].class));
        assertPermitted("*..* *(..)", Integer.class.getConstructor(int.class));
        assertNotPermitted("java.lang.String value*(..)", String.class.getConstructor(char[].class));
        assertNotPermitted("java.lang.String <constructor>(..)", String.class.getMethod("valueOf", char[].class));
    }

    @Test
    public void testClassFromAnotherClassLoader() throws NoSuchMethodException {
        // a proxy class lives in the class loader that defines it, so it cannot be found by name elsewhere
        final ClassLoader child = new URLClassLoader(new URL[0], getClass().getClassLoader());
        final Class<?> proxyClass = Proxy.newProxyInstance(child, new Class<?>[] {CharSequence.class},
                (proxy, method, args) -> null).getClass();
        Assert.assertEquals(child, proxyClass.getClassLoader());

        assertPermitted("java.lang.CharSequence length()", proxyClass.getMethod("length"));
        assertPermitted("java.lang.Object toString()", proxyClass.getMethod("toString"));
        assertNotPermitted("java.lang.CharSequence length()", proxyClass.getMethod("hashCode"));
        assertNotPermitted("java.lang.String length()", proxyClass.getMethod("length"));
    }

    @Test
    public void testPackagePrivateOverrides() throws Exception {
        final String pattern = PackagePrivateBase.class.getName() + " packagePrivate()";
        assertPermitted(pattern, PackagePrivateSub.class.getDeclaredMethod("packagePrivate"));
        assertPermitted(pattern, PackagePrivatePublicSub.class.getDeclaredMethod("packagePrivate"));

        // a method of the same name in another package does not override a package-private method
        assertNotPermitted(pattern, OtherPackageSub.class.getDeclaredMethod("packagePrivate"));

        // the same package name in another class loader is a different runtime package, so there is no override
        for (final Class<?> sub : List.of(PackagePrivateSub.class, PackagePrivatePublicSub.class)) {
            final Class<?> childSub = loadInChildLoader(sub);
            Assert.assertNotEquals(sub, childSub);
            Assert.assertEquals(PackagePrivateBase.class, childSub.getSuperclass());
            assertNotPermitted(pattern, childSub.getDeclaredMethod("packagePrivate"));
        }
    }

    private Class<?> loadInChildLoader(final Class<?> type) throws ClassNotFoundException {
        return new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                if (!name.equals(type.getName())) {
                    return super.loadClass(name, resolve);
                }
                final String resource = name.replace('.', '/') + ".class";
                try (final java.io.InputStream in = getParent().getResourceAsStream(resource)) {
                    final byte[] bytes = in.readAllBytes();
                    return defineClass(name, bytes, 0, bytes.length);
                } catch (java.io.IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
        }.loadClass(type.getName());
    }

    @Test
    public void testInvalidPatterns() {
        for (final String invalid : List.of("", "java.lang.String", "java.lang.String length", "length()",
                "java.lang.String length(", "java.lang.String len-gth()", "java.lang.String length(int,)",
                "java.lang.String length(java.lang.Object..., int)", "* toString()")) {
            Assert.assertThrows(invalid, UncheckedDeephavenException.class,
                    () -> new MethodListInvocationValidator(List.of(invalid)));
        }
    }

    private static void assertPermitted(final String pattern, final Executable executable) {
        Assert.assertEquals(pattern + " should match " + executable, Boolean.TRUE, permit(pattern, executable));
    }

    private static void assertNotPermitted(final String pattern, final Executable executable) {
        Assert.assertNull(pattern + " should not match " + executable, permit(pattern, executable));
    }

    private static Boolean permit(final String pattern, final Executable executable) {
        final MethodListInvocationValidator validator = new MethodListInvocationValidator(List.of(pattern));
        if (executable instanceof Method) {
            return validator.permitMethod((Method) executable);
        }
        return validator.permitConstructor((Constructor<?>) executable);
    }
}
