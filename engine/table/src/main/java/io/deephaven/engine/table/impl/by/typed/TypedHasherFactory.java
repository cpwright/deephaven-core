package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.*;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Produces typed hashers (e.g. byte * object * float * double * int) on-demand or using a set of pregenerated and
 * precompiled hashers for singleton and pairs of types.
 */
public class TypedHasherFactory {
    private static final boolean USE_PREGENERATED_HASHERS =
            Configuration.getInstance().getBooleanWithDefault("TypedHasherFactory.usePregeneratedHashers", true);

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param baseClass the base class (e.g. {@link StaticChunkedOperatorAggregationStateManagerTypedBase} that the
     *        generated hasher extends from
     * @param tableKeySources the key sources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @param <T> the base class
     * @return an instantiated hasher
     */
    public static <T> T make(Class<T> baseClass, ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        return make(hasherConfigForBase(baseClass), tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }

    private static class ProbeSpec {
        final String name;
        final String stateValueName;
        final Consumer<CodeBlock.Builder> found;
        final Consumer<CodeBlock.Builder> missing;
        final ParameterSpec [] params;

        private ProbeSpec(String name, String stateValueName, Consumer<CodeBlock.Builder> found, Consumer<CodeBlock.Builder> missing, ParameterSpec ... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.found = found;
            this.missing = missing;
            this.params = params;
        }
    }

    private static class BuildSpec {
        final String name;
        final String stateValueName;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> found;
        final BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert;
        final ParameterSpec [] params;

        private BuildSpec(String name, String stateValueName, BiConsumer<HasherConfig<?>, CodeBlock.Builder> found, BiConsumer<HasherConfig<?>, CodeBlock.Builder> insert, ParameterSpec ... params) {
            this.name = name;
            this.stateValueName = stateValueName;
            this.found = found;
            this.insert = insert;
            this.params = params;
        }
    }

    @NotNull
    public static <T> HasherConfig<T> hasherConfigForBase(Class<T> baseClass) {
        final String classPrefix;
        final String packageMiddle;
        final String mainStateName;
        final String overflowStateName;
        final String emptyStateName;
        final boolean openAddressed;
        final boolean openAddressedPivot;
        final Consumer<CodeBlock.Builder> moveMain;
        final List<BuildSpec> builds = new ArrayList<>();
        final List<ProbeSpec> probes = new ArrayList<>();
        if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
            classPrefix = "StaticAggHasher";
            packageMiddle = "staticagg";
            openAddressed = false;
            openAddressedPivot = false;
            mainStateName = "mainOutputPosition";
            overflowStateName = "overflowOutputPosition";
            emptyStateName = "EMPTY_OUTPUT_POSITION";
            moveMain = null;
        } else if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            classPrefix = "StaticAggOpenHasher";
            packageMiddle = "staticopenagg";
            openAddressed = true;
            openAddressedPivot = false;
            mainStateName = "mainOutputPosition";
            overflowStateName = null;
            emptyStateName = "EMPTY_OUTPUT_POSITION";
            moveMain = TypedHasherFactory::staticAggMoveMain;
            builds.add(new BuildSpec("build", "outputPosition", TypedHasherFactory::buildFound, TypedHasherFactory::buildInsert));
        } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
            classPrefix = "IncrementalAggHasher";
            packageMiddle = "incagg";
            openAddressed = false;
            openAddressedPivot = false;
            mainStateName = "mainOutputPosition";
            overflowStateName = "overflowOutputPosition";
            emptyStateName = "EMPTY_OUTPUT_POSITION";
            moveMain = null;
        } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            classPrefix = "IncrementalAggOpenHasher";
            packageMiddle = "incopenagg";
            openAddressed = true;
            openAddressedPivot = true;
            mainStateName = "mainOutputPosition";
            overflowStateName = null;
            emptyStateName = "EMPTY_OUTPUT_POSITION";
            moveMain = TypedHasherFactory::incAggMoveMain;

            final ClassName rowKeyType = ClassName.get(RowKeys.class);
            final ParameterizedTypeName emptiedChunkType = ParameterizedTypeName.get(ClassName.get(WritableIntChunk.class), rowKeyType);
            final ParameterSpec emptiedPositions = ParameterSpec.builder(emptiedChunkType, "emptiedPositions").build();;

            probes.add(new ProbeSpec("doRemoveProbe", "outputPosition", TypedHasherFactory::removeProbeFound, TypedHasherFactory::probeMissing, emptiedPositions));
            probes.add(new ProbeSpec("doModifyProbe", "outputPosition", TypedHasherFactory::probeFound, TypedHasherFactory::probeMissing));

            builds.add(new BuildSpec("build", "outputPosition", TypedHasherFactory::buildFoundIncrementalInitial, TypedHasherFactory::buildInsertIncremental));

            final ParameterSpec reincarnatedPositions = ParameterSpec.builder(emptiedChunkType, "reincarnatedPositions").build();;
            builds.add(new BuildSpec("buildForUpdate", "outputPosition", TypedHasherFactory::buildFoundIncrementalUpdate, TypedHasherFactory::buildInsertIncremental, reincarnatedPositions));
        } else {
            throw new UnsupportedOperationException("Unknown class to make: " + baseClass);
        }

        return new HasherConfig<>(baseClass, classPrefix, packageMiddle, openAddressed, openAddressedPivot, mainStateName,
                overflowStateName,
                emptyStateName, int.class, moveMain, probes, builds);
    }

    private static void staticAggMoveMain(CodeBlock.Builder builder) {
        builder.addStatement("outputPositionToHashSlot.set(destState[tableLocation], tableLocation)");
    }

    private static void incAggMoveMain(CodeBlock.Builder builder) {
        builder.addStatement("outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(destinationLocation), destinationLocation)");
    }

    private static void buildFound(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    private static void buildFoundIncremental(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFound(hasherConfig, builder);
        builder.addStatement("final long oldRowCount = rowCountSource.getUnsafe(outputPosition)");
        builder.addStatement("rowCountSource.set(outputPosition, oldRowCount + 1)");
    }

    private static void buildFoundIncrementalInitial(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFoundIncremental(hasherConfig, builder);
        builder.addStatement("Assert.gtZero(oldRowCount, \"oldRowCount\")");
    }

    private static void buildFoundIncrementalUpdate(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildFoundIncremental(hasherConfig, builder);
        builder.beginControlFlow("if (oldRowCount == 0)");
        builder.addStatement("reincarnatedPositions.add(outputPosition)");
        builder.endControlFlow();
    }

    private static void buildInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("outputPosition = nextOutputPosition.getAndIncrement()");
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
        builder.addStatement("$L.set(tableLocation, outputPosition)", hasherConfig.mainStateName);
        builder.addStatement("outputPositionToHashSlot.set(outputPosition, tableLocation)");
    }

    private static void buildInsertIncremental(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        buildInsert(hasherConfig, builder);
        builder.addStatement("rowCountSource.set(outputPosition, 1L)");
    }

    private static void probeFound(CodeBlock.Builder builder) {
        builder.addStatement("outputPositions.set(chunkPosition, outputPosition)");
    }

    private static void removeProbeFound(CodeBlock.Builder builder) {
        probeFound(builder);

        builder.addStatement("final long oldRowCount = rowCountSource.getUnsafe(outputPosition)");
        builder.addStatement("Assert.gtZero(oldRowCount, \"oldRowCount\")");
        builder.beginControlFlow("if (oldRowCount == 1)");
        builder.addStatement("emptiedPositions.add(outputPosition)");
        builder.endControlFlow();
        builder.addStatement("rowCountSource.set(outputPosition, oldRowCount - 1)");
    }

    private static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw new IllegalStateException($S)", "Missing value in probe");
    }

    public static class HasherConfig<T> {
        final Class<T> baseClass;
        public final String classPrefix;
        public final String packageMiddle;
        final boolean openAddressed;
        final boolean openAddressedPivot;
        final String mainStateName;
        final String overflowStateName;
        final String emptyStateName;
        final Class<?> stateType;
        final Consumer<CodeBlock.Builder> moveMain;
        private final List<ProbeSpec> probes;
        private final List<BuildSpec> builds;

        HasherConfig(Class<T> baseClass, String classPrefix, String packageMiddle, boolean openAddressed,
                     boolean openAddressedPivot, String mainStateName,
                     String overflowStateName,
                     String emptyStateName, Class<?> stateType, Consumer<CodeBlock.Builder> moveMain, List<ProbeSpec> probes, List<BuildSpec> builds) {
            this.baseClass = baseClass;
            this.classPrefix = classPrefix;
            this.packageMiddle = packageMiddle;
            this.openAddressed = openAddressed;
            this.openAddressedPivot = openAddressedPivot;
            this.mainStateName = mainStateName;
            this.overflowStateName = overflowStateName;
            this.emptyStateName = emptyStateName;
            this.stateType = stateType;
            this.moveMain = moveMain;
            this.probes = probes;
            this.builds = builds;
        }
    }

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param hasherConfig the configuration of the class to generate
     * @param tableKeySources the key sources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @param <T> the base class
     * @return an instantiated hasher
     */
    public static <T> T make(HasherConfig<T> hasherConfig, ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        if (USE_PREGENERATED_HASHERS) {
            if (hasherConfig.baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticopenagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incopenagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            }
        }

        final ChunkType[] chunkTypes =
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        final String className = hasherName(hasherConfig, chunkTypes);

        JavaFile javaFile = generateHasher(hasherConfig, chunkTypes, className, Optional.of(Modifier.PUBLIC));

        String[] javaStrings = javaFile.toString().split("\n");
        final String javaString =
                Arrays.stream(javaStrings).filter(s -> !s.startsWith("package ")).collect(Collectors.joining("\n"));

        final Class<?> clazz = CompilerTools.compile(className, javaString, packageName(hasherConfig.packageMiddle));
        if (!hasherConfig.baseClass.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a " + hasherConfig.baseClass.getCanonicalName());
        }

        final Class<? extends T> castedClass = (Class<? extends T>) clazz;

        T retVal;
        try {
            final Constructor<? extends T> constructor1 =
                    castedClass.getDeclaredConstructor(ColumnSource[].class, int.class, double.class, double.class);
            retVal = constructor1.newInstance(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new UncheckedDeephavenException("Could not instantiate " + castedClass.getCanonicalName(), e);
        }
        return retVal;
    }

    @NotNull
    public static String packageName(String packageMiddle) {
        return "io.deephaven.engine.table.impl.by.typed." + packageMiddle + ".gen";
    }

    @NotNull
    public static String hasherName(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        return hasherConfig.classPrefix
                + Arrays.stream(chunkTypes).map(Objects::toString).collect(Collectors.joining(""));
    }

    @NotNull
    public static <T> JavaFile generateHasher(final HasherConfig<T> hasherConfig,
            final ChunkType[] chunkTypes,
            final String className,
            Optional<Modifier> visibility) {
        final String packageName = packageName(hasherConfig.packageMiddle);
        final TypeSpec.Builder hasherBuilder =
                TypeSpec.classBuilder(className).addModifiers(Modifier.FINAL).superclass(hasherConfig.baseClass);
        visibility.ifPresent(hasherBuilder::addModifiers);

        hasherBuilder.addMethod(createConstructor(hasherConfig, chunkTypes, hasherBuilder));

        if (hasherConfig.openAddressed) {
            hasherBuilder.addMethod(createNextTableLocationMethod(hasherConfig));
            hasherConfig.builds.forEach(bs -> hasherBuilder.addMethod(createBuildMethodForOpenAddressed(hasherConfig, bs, chunkTypes)));
            hasherConfig.probes.forEach(ps -> hasherBuilder.addMethod(createProbeMethodForPivot(hasherConfig, ps, chunkTypes)));
        } else {
            hasherBuilder.addMethod(createBuildMethodForOverflow(hasherConfig, chunkTypes));
            hasherBuilder.addMethod(createProbeMethodForOverflow(hasherConfig, chunkTypes));
        }
        hasherBuilder.addMethod(createHashMethod(chunkTypes));

        if (hasherConfig.openAddressed) {
            if (hasherConfig.openAddressedPivot) {
                hasherBuilder.addMethod(createMigrateLocationMethod(hasherConfig, chunkTypes));
                hasherBuilder.addMethod(createRehashInternalPartialMethod(hasherConfig, chunkTypes));
            } else {
                hasherBuilder.addMethod(createRehashInternalFullMethod(hasherConfig, chunkTypes));
            }
        } else {
            hasherBuilder.addMethod(createRehashBucketMethod(hasherConfig, chunkTypes));
            hasherBuilder.addMethod(createMaybeMoveMainBucket(hasherConfig, chunkTypes));
        }

        if (!hasherConfig.openAddressed) {
            hasherBuilder.addMethod(createFindOverflow(chunkTypes));
        }
        hasherBuilder.addMethod(createFindPositionForKey(hasherConfig, chunkTypes));

        final TypeSpec hasher = hasherBuilder.build();

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, hasher).indent("    ");
        fileBuilder.addFileComment("DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY "
                + TypedHasherFactory.class.getCanonicalName() + "\n" +
                "Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n");

        for (ChunkType chunkType : chunkTypes) {
            fileBuilder.addStaticImport(
                    ClassName.get(CharComparisons.class.getPackageName(), chunkType.name() + "Comparisons"), "eq");
        }
        return fileBuilder.build();
    }

    private static MethodSpec createNextTableLocationMethod(HasherConfig<?> hasherConfig) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("nextTableLocation").addParameter(int.class, "tableLocation")
                .returns(int.class).addModifiers(Modifier.PRIVATE);
        if (hasherConfig.openAddressedPivot) {
                builder.addStatement("return tableLocation == tableHashPivot - 1 ? 0 : (tableLocation + 1)");
        } else {
            builder.addStatement("return (tableLocation + 1) & (tableSize - 1)");
        }
        return builder.build();
    }

    @NotNull
    private static <T> MethodSpec createConstructor(HasherConfig<T> hasherConfig, ChunkType[] chunkTypes,
            TypeSpec.Builder hasherBuilder) {
        CodeBlock.Builder constructorCodeBuilder = CodeBlock.builder();
        if (hasherConfig.openAddressed && !hasherConfig.openAddressedPivot) {
            constructorCodeBuilder.addStatement("super(tableKeySources, tableSize, maximumLoadFactor)");
        } else {
            constructorCodeBuilder
                    .addStatement("super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        }
        addKeySourceFields(hasherConfig, chunkTypes, hasherBuilder, constructorCodeBuilder);

        return MethodSpec.constructorBuilder().addParameter(ColumnSource[].class, "tableKeySources")
                .addParameter(int.class, "tableSize").addParameter(double.class, "maximumLoadFactor")
                .addParameter(double.class, "targetLoadFactor").addModifiers(Modifier.PUBLIC)
                .addCode(constructorCodeBuilder.build())
                .build();
    }

    private static void addKeySourceFields(HasherConfig hasherConfig, ChunkType[] chunkTypes,
            TypeSpec.Builder hasherBuilder,
            CodeBlock.Builder constructorCodeBuilder) {
        final List<FieldSpec> keySources = new ArrayList<>();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> type =
                    hasherConfig.openAddressed && !hasherConfig.openAddressedPivot ? flatSourceType(chunkTypes[ii]) : arraySourceType(chunkTypes[ii]);
            keySources.add(
                    FieldSpec.builder(type, "mainKeySource" + ii).addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                            .build());
            constructorCodeBuilder.addStatement("this.mainKeySource$L = ($T) super.mainKeySources[$L]", ii, type, ii);
            if (hasherConfig.openAddressed) {
                constructorCodeBuilder.addStatement("this.mainKeySource$L.ensureCapacity(tableSize)", ii);
            } else {
                keySources.add(FieldSpec.builder(type, "overflowKeySource" + ii)
                        .addModifiers(Modifier.PRIVATE, Modifier.FINAL).build());
                constructorCodeBuilder.addStatement("this.overflowKeySource$L = ($T) super.overflowKeySources[$L]", ii,
                        type, ii);
            }
        }
        keySources.forEach(hasherBuilder::addField);
    }

    @NotNull
    private static MethodSpec createFindOverflow(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("findOverflow").addParameter(HashHandler.class, "handler");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addParameter(elementType(chunkTypes[ii]), "k" + ii);
        }

        builder.addParameter(int.class, "chunkPosition").addParameter(int.class, "overflowLocation")
                .returns(boolean.class).addModifiers(Modifier.PRIVATE)
                .beginControlFlow("while (overflowLocation != $T.NULL_INT)", QueryConstants.class)
                .beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")")
                .addStatement("handler.doOverflowFound(overflowLocation, chunkPosition)")
                .addStatement("return true")
                .endControlFlow()
                .addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)")
                .endControlFlow()
                .addStatement("return false");
        return builder.build();
    }

    @NotNull
    private static MethodSpec createFindPositionForKey(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("findPositionForKey").addParameter(Object.class, "key")
                .returns(int.class).addModifiers(Modifier.PUBLIC).addAnnotation(Override.class);
        if (chunkTypes.length != 1) {
            builder.addStatement("final Object [] ka = (Object[])key");
            for (int ii = 0; ii < chunkTypes.length; ++ii) {
                final Class<?> element = elementType(chunkTypes[ii]);
                unboxKey(builder, ii, element);
            }
        } else {
            final Class<?> element = elementType(chunkTypes[0]);
            unboxKey(builder, element);
        }

        builder.addStatement("int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");

        if (hasherConfig.openAddressed) {
            findPositionForKeyOpenAddressed(hasherConfig, chunkTypes, builder);
        } else {
            findPositionForKeyOverflow(hasherConfig, chunkTypes, builder);
        }
        return builder.build();
    }

    private static void findPositionForKeyOpenAddressed(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            MethodSpec.Builder builder) {
        builder.addStatement("int tableLocation = hashToTableLocation(hash)");

        builder.addStatement("final int lastTableLocation = nextTableLocation(tableLocation)");

        builder.beginControlFlow("while (true)");

        builder.addStatement("final int positionValue = $L.getUnsafe(tableLocation)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (positionValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return -1");
        builder.endControlFlow();

        builder.beginControlFlow("if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("return positionValue");
        builder.endControlFlow();

        builder.addStatement("$T.neq(tableLocation, $S, lastTableLocation, $S)", Assert.class, "tableLocation",
                "lastTableLocation");
        builder.addStatement("tableLocation = nextTableLocation(tableLocation)");

        builder.endControlFlow();
    }

    private static void findPositionForKeyOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            MethodSpec.Builder builder) {
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.addStatement("final int positionValue = $L.getUnsafe(tableLocation)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (positionValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return -1");
        builder.endControlFlow();

        builder.beginControlFlow("if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("return positionValue");
        builder.endControlFlow();

        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")");
        builder.addStatement("return $L.getUnsafe(overflowLocation)", hasherConfig.overflowStateName);
        builder.endControlFlow();

        builder.addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        builder.endControlFlow();
        builder.addStatement("return -1");
    }

    private static void unboxKey(MethodSpec.Builder builder, int ii, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k$L = ka[$L]", element, ii, ii);
        } else {
            builder.addStatement("final $T k$L = $T.unbox(($T)ka[$L])", element, ii, TypeUtils.class,
                    TypeUtils.getBoxedType(element), ii);
        }
    }

    private static void unboxKey(MethodSpec.Builder builder, Class<?> element) {
        if (element == Object.class) {
            builder.addStatement("final $T k0 = key", element);
        } else {
            builder.addStatement("final $T k0 = $T.unbox(($T)key)", element, TypeUtils.class,
                    TypeUtils.getBoxedType(element));
        }
    }

    @NotNull
    private static MethodSpec createMaybeMoveMainBucket(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder =
                MethodSpec.methodBuilder("maybeMoveMainBucket").addParameter(HashHandler.class, "handler")
                        .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                        .addParameter(int.class, "bucketsToAdd").returns(int.class).addModifiers(Modifier.PRIVATE);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = mainKeySource$L.getUnsafe(sourceBucket)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash)");
        builder.addStatement("final int mainInsertLocation");
        builder.beginControlFlow("if (location == sourceBucket)");
        builder.addStatement("mainInsertLocation = destBucket");
        builder.addStatement("$L.set(destBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.nextControlFlow("else");
        builder.addStatement("mainInsertLocation = sourceBucket");
        builder.addStatement("$L.set(destBucket, $L.getUnsafe(sourceBucket))", hasherConfig.mainStateName,
                hasherConfig.mainStateName);
        builder.addStatement("$L.set(sourceBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(destBucket, k$L)", ii, ii);
            builder.addStatement("mainKeySource$L.set(sourceBucket, $L)", ii, elementNull(chunkTypes[ii]));
        }
        builder.addStatement("handler.doMoveMain(sourceBucket, destBucket)");
        builder.endControlFlow();
        builder.addStatement("return mainInsertLocation");

        return builder
                .build();
    }

    @NotNull
    private static MethodSpec createRehashBucketMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final int position = $L.getUnsafe(sourceBucket)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (position == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return");
        builder.endControlFlow();

        builder.addStatement(
                "int mainInsertLocation = maybeMoveMainBucket(handler, sourceBucket, destBucket, bucketsToAdd)");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(sourceBucket)");
        builder.addStatement("mainOverflowLocationSource.set(sourceBucket, QueryConstants.NULL_INT)");
        builder.addStatement("mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.addStatement(
                "final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T overflowKey$L = overflowKeySource$L.getUnsafe(overflowLocation)",
                    elementType(chunkTypes[ii]), ii, ii);
        }
        builder.addStatement("final int overflowHash = hash(" + IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "overflowKey" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement(
                "final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash)");
        builder.beginControlFlow("if (overflowTableLocation == mainInsertLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(mainInsertLocation, overflowKey$L)", ii, ii);
        }
        builder.addStatement("$L.set(mainInsertLocation, $L.getUnsafe(overflowLocation))", hasherConfig.mainStateName,
                hasherConfig.overflowStateName);
        builder.addStatement("handler.doPromoteOverflow(overflowLocation, mainInsertLocation)");
        builder.addStatement("$L.set(overflowLocation, QueryConstants.NULL_INT)", hasherConfig.overflowStateName);

        // key source loop
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(overflowLocation, $L)", ii, elementNull(chunkTypes[ii]));
        }

        builder.addStatement("freeOverflowLocation(overflowLocation)");
        builder.addStatement("mainInsertLocation = -1");
        builder.nextControlFlow("else");
        builder.addStatement(
                "final int oldOverflowLocation = mainOverflowLocationSource.getUnsafe(overflowTableLocation)");
        builder.addStatement("mainOverflowLocationSource.set(overflowTableLocation, overflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation)");
        builder.endControlFlow();
        builder.addStatement("overflowLocation = nextOverflowLocation");
        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashBucket").addParameter(HashHandler.class, "handler")
                .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                .addParameter(int.class, "bucketsToAdd").returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    // this rehash internal method is not incremental, which makes us nervous about using it for incremental updates
    // we are also using the old arrays as temporary space, so any moving of objects must be done inline (but the
    // single inheritor we have does not do that, so we are not adding that complexity at this instant)
    @NotNull
    private static MethodSpec createRehashInternalFullMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final int oldSize = tableSize >> 1");

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T[] destKeyArray$L = new $T[tableSize]", elementType(chunkTypes[ii]), ii,
                    elementType(chunkTypes[ii]));
        }
        builder.addStatement("final $T[] destState = new $T[tableSize]", hasherConfig.stateType,
                hasherConfig.stateType);
        builder.addStatement("$T.fill(destState, $L)", Arrays.class, hasherConfig.emptyStateName);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T [] originalKeyArray$L = mainKeySource$L.getArray()",
                    elementType(chunkTypes[ii]), ii, ii);
            builder.addStatement("mainKeySource$L.setArray(destKeyArray$L)", ii, ii);
        }
        builder.addStatement("final $T [] originalStateArray = $L.getArray()", hasherConfig.stateType,
                hasherConfig.mainStateName);
        builder.addStatement("$L.setArray(destState)", hasherConfig.mainStateName);

        builder.beginControlFlow("for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket)");
        builder.beginControlFlow("if (originalStateArray[sourceBucket] == $L)", hasherConfig.emptyStateName);
        builder.addStatement("continue");
        builder.endControlFlow();

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = originalKeyArray$L[sourceBucket]", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("int tableLocation = hashToTableLocation(hash)");
        builder.addStatement("final int lastTableLocation = (tableLocation + tableSize - 1) & (tableSize - 1)");
        builder.beginControlFlow("while (true)");
        builder.beginControlFlow("if (destState[tableLocation] == $L)", hasherConfig.emptyStateName);
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("destKeyArray$L[tableLocation] = k$L", ii, ii);
        }
        builder.addStatement("destState[tableLocation] = originalStateArray[sourceBucket]", hasherConfig.mainStateName);
        builder.beginControlFlow("if (sourceBucket != tableLocation)");
        hasherConfig.moveMain.accept(builder);
        builder.endControlFlow();
        builder.addStatement("break");
        builder.nextControlFlow("else");
        builder.addStatement("$T.neq(tableLocation, $S, lastTableLocation, $S)", Assert.class, "tableLocation",
                "lastTableLocation");
        builder.addStatement("tableLocation = (tableLocation + 1) & (tableSize - 1)");
        builder.endControlFlow();
        builder.endControlFlow();

        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashInternal")
                .returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createRehashInternalPartialMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        // ensure the capacity for everything
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.ensureCapacity(tableHashPivot + bucketsToAdd)", ii);
        }
        builder.addStatement("$L.ensureCapacity(tableHashPivot + bucketsToAdd)", hasherConfig.mainStateName);

        builder.addStatement("final boolean lastValueExists = $L.getUnsafe(tableHashPivot - 1) != $L", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("final int firstLocationToMigrate = tableHashPivot - (tableSize >> 1)");
        builder.addStatement("int lastLocationToMigrate = firstLocationToMigrate + bucketsToAdd");
        builder.addStatement("final int frontLocationsToMigrate;");
        builder.beginControlFlow("if (lastLocationToMigrate >= tableSize)");
        builder.addStatement("frontLocationsToMigrate = lastLocationToMigrate - tableSize + 1");
        builder.addStatement("lastLocationToMigrate = tableSize");
        builder.nextControlFlow("else");
        builder.addStatement("frontLocationsToMigrate = lastValueExists ? 1 : 0");
        builder.endControlFlow();

        builder.addStatement("tableHashPivot += bucketsToAdd");

        builder.beginControlFlow("for (int locationToMigrate = firstLocationToMigrate; locationToMigrate < lastLocationToMigrate || (locationToMigrate < tableSize && $L.getUnsafe(locationToMigrate) != $L); ++locationToMigrate)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("migrateOneLocation(locationToMigrate)");
        builder.endControlFlow();

        builder.beginControlFlow("for (int locationToMigrate = 0; locationToMigrate < frontLocationsToMigrate || (locationToMigrate < firstLocationToMigrate && $L.getUnsafe(locationToMigrate) != $L); ++locationToMigrate)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("migrateOneLocation(locationToMigrate)");
        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashInternal")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addParameter(int.class, "bucketsToAdd")
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createMigrateLocationMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.beginControlFlow("if ($L.getUnsafe(locationToMigrate) == $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("return");
        builder.endControlFlow();

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = mainKeySource$L.getUnsafe(locationToMigrate)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");

        builder.addStatement("int destinationLocation = hashToTableLocation(hash)");
        builder.beginControlFlow("if (destinationLocation == locationToMigrate)");
        builder.addStatement("return");
        builder.endControlFlow();

        builder.beginControlFlow("while ($L.getUnsafe(destinationLocation) != $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.addStatement("destinationLocation = nextTableLocation(destinationLocation)");
        // TODO: if we could wrap then we might have problems, because we'll then migrate front locations think about
        //  what to do in that case.  Or, alternatively this deserves a mini-dissertation on why we would never need to
        //  wrap around to the beginning based on adding enough slots to accomodate the values that we would like to
        //  rehash
        builder.endControlFlow();

        doRehashMoveMain(hasherConfig, chunkTypes, builder, "locationToMigrate", "destinationLocation");

        return MethodSpec.methodBuilder("migrateOneLocation")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addParameter(int.class, "locationToMigrate")
                .addCode(builder.build()).build();
    }

    private static void doRehashMoveMain(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes, CodeBlock.Builder builder, final String sourceLocation, final String destinationLocation) {
        // we need to move the keys, states, and call the move main
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set($L, mainKeySource$L.getUnsafe($L))", ii, destinationLocation, ii, sourceLocation);
            if (chunkTypes[ii] == ChunkType.Object) {
                builder.addStatement("mainKeySource$L.set($L, null)", ii, sourceLocation);
            }
        }
        builder.addStatement("$L.set($L, $L.getUnsafe($L))", hasherConfig.mainStateName, destinationLocation, hasherConfig.mainStateName, sourceLocation);
        builder.addStatement("$L.set($L, $L)", hasherConfig.mainStateName, sourceLocation, hasherConfig.emptyStateName);
        hasherConfig.moveMain.accept(builder);
    }

    @NotNull
    private static MethodSpec createBuildMethodForOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName,
                hasherConfig.emptyStateName);
        builder.addStatement("numEntries++");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(tableLocation, k$L)", ii, ii);
        }
        builder.addStatement("handler.doMainInsert(tableLocation, chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("final int newOverflowLocation = allocateOverflowLocation()");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(newOverflowLocation, k$L)", ii, ii);
        }
        builder.addStatement("mainOverflowLocationSource.set(tableLocation, newOverflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation)");
        builder.addStatement("numEntries++");
        builder.addStatement("handler.doOverflowInsert(newOverflowLocation, chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();


        return MethodSpec.methodBuilder("build")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createBuildMethodForOpenAddressed(HasherConfig<?> hasherConfig, BuildSpec buildSpec, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int firstTableLocation = hashToTableLocation(hash)");
        builder.addStatement("int tableLocation = firstTableLocation");
        builder.beginControlFlow("while (true)");
        builder.addStatement("$T $L = $L.getUnsafe(tableLocation)", hasherConfig.stateType, buildSpec.stateValueName,
                hasherConfig.mainStateName);
        builder.beginControlFlow("if ($L == $L)", buildSpec.stateValueName, hasherConfig.emptyStateName);
        builder.addStatement("numEntries++");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(tableLocation, k$L)", ii, ii);
        }
        buildSpec.insert.accept(hasherConfig, builder);
        builder.addStatement("break");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        buildSpec.found.accept(hasherConfig, builder);
        builder.addStatement("break");
        builder.nextControlFlow("else");
        builder.addStatement("tableLocation = nextTableLocation(tableLocation)");
        builder.addStatement("$T.neq(tableLocation, $S, firstTableLocation, $S)", Assert.class, "tableLocation",
                "firstTableLocation");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(buildSpec.name)
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks");
        for (final ParameterSpec param : buildSpec.params) {
            methodBuilder.addParameter(param);
        }
        return methodBuilder
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
//                .addAnnotation(Override.class)
                .build();
    }

    private static MethodSpec createProbeMethodForPivot(HasherConfig<?> hasherConfig, ProbeSpec ps, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }

        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int firstTableLocation = hashToTableLocation(hash)");
        builder.addStatement("int tableLocation = firstTableLocation");
        builder.addStatement("boolean found = false");

        builder.addStatement("$T $L", hasherConfig.stateType, ps.stateValueName);
        builder.beginControlFlow("while (($L = $L.getUnsafe(tableLocation)) != $L)", ps.stateValueName, hasherConfig.mainStateName,
                hasherConfig.emptyStateName);

        builder.beginControlFlow("if (" + getEqualsStatement(chunkTypes) + ")");
        ps.found.accept(builder);
        builder.addStatement("found = true");
        builder.addStatement("break");
        builder.endControlFlow();
        builder.addStatement("tableLocation = nextTableLocation(tableLocation)");
        builder.addStatement("$T.neq(tableLocation, $S, firstTableLocation, $S)", Assert.class, "tableLocation", "firstTableLocation");
        builder.endControlFlow();

        builder.beginControlFlow("if (!found)");
        ps.missing.accept(builder);
        builder.endControlFlow();


        builder.endControlFlow();

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(ps.name)
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks");

        for (final ParameterSpec param : ps.params) {
            methodBuilder.addParameter(param);
        }

        methodBuilder.returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build());
        //                .addAnnotation(Override.class)

        return methodBuilder.build();
    }

    @NotNull
    private static MethodSpec createProbeMethodForOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName,
                hasherConfig.emptyStateName);
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();

        return MethodSpec.methodBuilder("probe")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }



    @NotNull
    private static MethodSpec createHashMethod(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("hash").returns(int.class).addModifiers(Modifier.PRIVATE, Modifier.STATIC);
        final CodeBlock.Builder hashCodeBuilder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ChunkType chunkType = chunkTypes[ii];
            builder.addParameter(elementType(chunkType), "k" + ii);
            if (ii == 0) {
                hashCodeBuilder.addStatement("int hash = $T.hashInitialSingle(k0)", chunkHasherByChunkType(chunkType));
            } else {
                hashCodeBuilder.addStatement("hash = $T.hashUpdateSingle(hash, k" + ii + ")",
                        chunkHasherByChunkType(chunkType));
            }
        }
        hashCodeBuilder.addStatement("return hash");
        return builder.addCode(hashCodeBuilder.build()).build();
    }

    @NotNull
    private static String getEqualsStatement(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(mainKeySource" + x + ".getUnsafe(tableLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    @NotNull
    private static String getEqualsStatementOverflow(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(overflowKeySource" + x + ".getUnsafe(overflowLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    private static ClassName chunkHasherByChunkType(ChunkType chunkType) {
        return ClassName.get(CharChunkHasher.class.getPackageName(), chunkType.name() + "ChunkHasher");
    }

    static Class<? extends ArrayBackedColumnSource> arraySourceType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return CharacterArraySource.class;
            case Byte:
                return ByteArraySource.class;
            case Short:
                return ShortArraySource.class;
            case Int:
                return IntegerArraySource.class;
            case Long:
                return LongArraySource.class;
            case Float:
                return FloatArraySource.class;
            case Double:
                return DoubleArraySource.class;
            case Object:
                return ObjectArraySource.class;
        }
    }

    static Class<? extends ColumnSource> flatSourceType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return ImmutableCharArraySource.class;
            case Byte:
                return ImmutableByteArraySource.class;
            case Short:
                return ImmutableShortArraySource.class;
            case Int:
                return ImmutableIntArraySource.class;
            case Long:
                return ImmutableLongArraySource.class;
            case Float:
                return ImmutableFloatArraySource.class;
            case Double:
                return ImmutableDoubleArraySource.class;
            case Object:
                return ImmutableObjectArraySource.class;
        }
    }

    static Class<?> elementType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return char.class;
            case Byte:
                return byte.class;
            case Short:
                return short.class;
            case Int:
                return int.class;
            case Long:
                return long.class;
            case Float:
                return float.class;
            case Double:
                return double.class;
            case Object:
                return Object.class;
        }
    }

    private static String elementNull(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
            case Byte:
            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
                return "QueryConstants.NULL_" + chunkType.name().toUpperCase();
            case Object:
                return "null";
        }
    }
}
