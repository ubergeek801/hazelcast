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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.function.TriFunction;

import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static java.lang.Math.max;

public class BroadcastJoinP<T, U, K, S, R> extends TransformStatefulP<T, K, S, R> {
    private final ToLongFunction<? super U> broadcastTimestampFn;
    private final TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> broadcastFlatMapFn;
    private final FlatMapper<U, R> broadcastEventMapper = flatMapper(this::broadcastEvent);

    public BroadcastJoinP(
            long ttl,
            @Nonnull Function<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunction<? super T> timestampFn,
            @Nonnull ToLongFunction<? super U> broadcastTimestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super U, ? extends Traverser<R>> broadcastFlatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn) {
        super(ttl, keyFn, timestampFn, createFn, statefulFlatMapFn, onEvictFn);
        this.broadcastTimestampFn = broadcastTimestampFn;
        this.broadcastFlatMapFn = broadcastFlatMapFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (ordinal == 1) {
            return broadcastEventMapper.tryProcess((U) item);
        }
        return super.tryProcess(ordinal, item);
    }

    @Nonnull
    private Traverser<R> broadcastEvent(U event) {
        // TODO this logic is common with TransformStatefulP#flatMapEvent()
        long timestamp = broadcastTimestampFn.applyAsLong(event);
        if (timestamp < currentWm && ttl != Long.MAX_VALUE) {
            logLateEvent(getLogger(), currentWm, event);
            lateEventsDropped.inc();
            return Traversers.empty();
        }

        // TODO probably there's a better way to emit items than to buffer them here
        ArrayList<R> output = new ArrayList<>();
        broadcastFlatMapFn.apply(null, null, event);
        keyToState.forEach((k, v) -> {
            v.setTimestamp(max(v.timestamp(), timestamp));
            S state = v.item();
            Traverser<R> items = broadcastFlatMapFn.apply(state, k, event);
            while (true) {
                R item = items.next();
                if (item == null) {
                    break;
                }
                output.add(item);
            }
        });

        return Traversers.traverseIterable(output);
    }
}
