/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.incrementalJoinP;
import static java.util.Arrays.asList;

public class IncrementalJoinTransform<T, U, K, S, R> extends FlatMapStatefulTransform<T, K, S, R> {
    private static final long serialVersionUID = 1L;

    private final FunctionEx<? super U, ? extends K> keyFn1;
    private final ToLongFunctionEx<? super U> timestampFn1;
    private final TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> statefulFlatMapFn1;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public IncrementalJoinTransform(
            @Nonnull Transform upstream,
            @Nonnull Transform upstream1,
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull FunctionEx<? super U, ? extends K> keyFn1,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull ToLongFunctionEx<? super U> timestampFn1,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> flatMapFn1,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn) {
        super(asList(upstream, upstream1), ttl, keyFn, timestampFn, createFn, flatMapFn, onEvictFn);
        this.keyFn1 = keyFn1;
        this.timestampFn1 = timestampFn1;
        statefulFlatMapFn1 = flatMapFn1;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, false);
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(),
                incrementalJoinP(ttl, keyFn, keyFn1, timestampFn, timestampFn1, createFn,
                        statefulFlatMapFn, statefulFlatMapFn1, onEvictFn));
        p.addEdges(this, pv.v, edge -> {
            if (edge.getDestOrdinal() == 0) {
                edge.partitioned(keyFn);
            } else {
                edge.partitioned(keyFn1);
            }
            edge.distributed();
        });
    }
}
