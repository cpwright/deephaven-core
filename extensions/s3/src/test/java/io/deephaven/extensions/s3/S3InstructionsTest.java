//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class S3InstructionsTest {

    @Test
    void defaults() {
        final S3Instructions instructions = S3Instructions.builder().build();
        assertThat(instructions.regionName().isEmpty()).isTrue();
        assertThat(instructions.maxConcurrentRequests()).isEqualTo(256);
        assertThat(instructions.readAheadCount()).isEqualTo(32);
        assertThat(instructions.fragmentSize()).isEqualTo(65536);
        assertThat(instructions.connectionTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.readTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(instructions.credentials()).isEqualTo(Credentials.defaultCredentials());
        assertThat(instructions.writePartSize()).isEqualTo(10485760);
        assertThat(instructions.numConcurrentWriteParts()).isEqualTo(64);
        assertThat(instructions.endpointOverride()).isEmpty();
    }

    @Test
    void testSetRegion() {
        final Optional<String> region = S3Instructions.builder()
                .regionName("some-region")
                .build()
                .regionName();
        assertThat(region.isPresent()).isTrue();
        assertThat(region.get()).isEqualTo("some-region");
    }

    @Test
    void testSetMaxConcurrentRequests() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .maxConcurrentRequests(100)
                .build()
                .maxConcurrentRequests())
                .isEqualTo(100);
    }

    @Test
    void testMinMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .maxConcurrentRequests(-1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    void tooSmallMaxConcurrentRequests() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .maxConcurrentRequests(0)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("maxConcurrentRequests");
        }
    }

    @Test
    void minReadAheadCount() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .readAheadCount(0)
                .build()
                .readAheadCount())
                .isZero();
    }

    @Test
    void tooSmallReadAheadCount() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .readAheadCount(-1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("readAheadCount");
        }
    }

    @Test
    void minFragmentSize() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .fragmentSize(8 * (1 << 10))
                .build()
                .fragmentSize())
                .isEqualTo(8 * (1 << 10));
    }

    @Test
    void tooSmallFragmentSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .fragmentSize(8 * (1 << 10) - 1)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("fragmentSize");
        }
    }

    @Test
    void basicCredentials() {
        assertThat(S3Instructions.builder()
                .regionName("some-region")
                .credentials(Credentials.basic("foo", "bar"))
                .build()
                .credentials())
                .isEqualTo(Credentials.basic("foo", "bar"));
    }

    @Test
    void badCredentials() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .credentials(new Credentials() {})
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("credentials");
        }
    }

    @Test
    void tooSmallWritePartSize() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .writePartSize(1024)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("writePartSize");
        }
    }

    @Test
    void tooSmallNumConcurrentWriteParts() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .numConcurrentWriteParts(0)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("numConcurrentWriteParts");
        }
    }

    @Test
    void tooLargeNumConcurrentWriteParts() {
        try {
            S3Instructions.builder()
                    .regionName("some-region")
                    .numConcurrentWriteParts(1001)
                    .maxConcurrentRequests(1000)
                    .build();
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessageContaining("numConcurrentWriteParts");
        }
    }
}
