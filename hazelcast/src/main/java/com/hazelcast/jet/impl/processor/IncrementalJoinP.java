/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.TriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

public class IncrementalJoinP<T, U, K, S, R> extends TransformStatefulP<T, K, S, R> {
    private final Function<? super U, ? extends K> keyFn1;
    private final ToLongFunction<? super U> timestampFn1;
    private final TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> statefulFlatMapFn1;
    private final FlatMapper<U, R> flatMapper1 = flatMapper(this::flatMapEvent1);

    public IncrementalJoinP(
            long ttl,
            @Nonnull Function<? super T, ? extends K> keyFn,
            @Nonnull Function<? super U, ? extends K> keyFn1,
            @Nonnull ToLongFunction<? super T> timestampFn,
            @Nonnull ToLongFunction<? super U> timestampFn1,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> statefulFlatMapFn1,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn) {
        super(ttl, keyFn, timestampFn, createFn, statefulFlatMapFn, onEvictFn);
        this.keyFn1 = keyFn1;
        this.timestampFn1 = timestampFn1;
        this.statefulFlatMapFn1 = statefulFlatMapFn1;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (ordinal == 1) {
            return flatMapper1.tryProcess((U) item);
        }
        return super.tryProcess(ordinal, item);
    }

    @Nonnull
    private Traverser<R> flatMapEvent1(U event) {
        return flatMapEvent(event, keyFn1, timestampFn1, statefulFlatMapFn1);
    }
}
