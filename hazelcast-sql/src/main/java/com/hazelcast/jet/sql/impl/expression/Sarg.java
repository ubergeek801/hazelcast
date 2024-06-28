/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.expression;

import com.google.common.collect.RangeSet;
import com.hazelcast.sql.impl.expression.AbstractSarg;

import java.io.Serializable;

@SuppressWarnings("UnstableApiUsage")
public class Sarg<C extends Comparable<C>> implements AbstractSarg<C>, Serializable {

    private final RangeSet<C> set;
    private final Boolean nullAs;

    public Sarg(RangeSet<C> set, Boolean nullAs) {
        this.set = set;
        this.nullAs = nullAs;
    }

    @Override
    public Boolean contains(C value) {
        if (value == null) {
            return nullAs;
        }
        return set.contains(value);
    }
}