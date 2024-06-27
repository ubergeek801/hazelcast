/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster;

import com.hazelcast.version.Version;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("com.hazelcast.test.annotation.QuickTest")
class VersionsTest {
    @Test
    void testConstructor() {
        assertUtilityConstructor(Versions.class);
    }

    @Test
    void version_4_0() {
        assertEquals(Versions.V4_0, Version.of(4, 0));
    }

    @Test
    void version_4_1() {
        assertEquals(Versions.V4_1, Version.of(4, 1));
    }

    @Test
    void testParse() {
        Version version = Versions.CURRENT_CLUSTER_VERSION;
        assertEquals(version, Version.of(version.toString()));
    }

    @Test
    void testCurrentVersion() {
        assertNotNull(Versions.CURRENT_CLUSTER_VERSION);
        assertNotNull(Versions.PREVIOUS_CLUSTER_VERSION);

        assertNotEquals(Versions.PREVIOUS_CLUSTER_VERSION, Versions.CURRENT_CLUSTER_VERSION);
    }
}
