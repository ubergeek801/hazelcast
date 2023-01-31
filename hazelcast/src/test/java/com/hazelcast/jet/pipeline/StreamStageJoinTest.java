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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.TriFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StreamStageJoinTest extends PipelineStreamTestSupport {
    @Test
    public void incrementalJoin() {
      // Given
      List<Integer> input1 = sequence(itemCount);
      List<Integer> input2 = sequence(itemCount);

      // When
      StreamStageWithKey<Integer, Integer> inputStream1 =
              streamStageFromList(input1).groupingKey(i -> i);
      StreamStageWithKey<String, Integer> inputStream2 =
              streamStageFromList(input2).map(String::valueOf)
                      .groupingKey(Integer::parseInt);

      TriFunction<Map<Integer, Long>, Integer, Integer, Traverser<Long>> flatMapFn1 =
              (integerLongMap, key, value) -> {
                  Long count = integerLongMap.get(value);
                  if (count == null) {
                      integerLongMap.put(value, 1L);
                      // received the first value; don't emit anything yet
                      return Traversers.empty();
                  }
                  // received the second value, so emit
                  return Traversers.singleton((long) value);
              };
      TriFunction<Map<Integer, Long>, Integer, String, Traverser<Long>> flatMapFn2 =
              (integerLongMap, key, value) -> {
                  int mapKey = Integer.parseInt(value);
                  Long count = integerLongMap.get(mapKey);
                  if (count == null) {
                      integerLongMap.put(mapKey, 1L);
                      // received the first value; don't emit anything yet
                      return Traversers.empty();
                  }
                  // received the second value, so emit
                  return Traversers.singleton(Long.parseLong(value));
              };
      StreamStage<Long> outputStream =
              inputStream1.incrementalJoin(HashMap::new, flatMapFn1, inputStream2, flatMapFn2)
                      .setLocalParallelism(4);

      // Then
      outputStream.writeTo(sink);
      execute();
      Function<Long, String> formatFn = i -> String.format("%04d", i);
      assertEquals(
              streamToString(input1.stream().map(i -> (long) i), formatFn),
              streamToString(sinkStreamOf(Long.class), formatFn)
      );
    }

    @Test
    public void broadcastJoin() {
        // Given
        List<Integer> keyedInput = sequence(itemCount);
        List<Integer> broadcastInput = sequence(100);
        Set<Integer> allBroadcastItems = new HashSet<>();

        // When
        StreamStageWithKey<Integer, Integer> keyedInputStream =
                streamStageFromList(keyedInput).groupingKey(i -> i);
        StreamStage<Integer> broadcastInputStream = streamStageFromList(broadcastInput);

        TriFunction<Set<Integer>, Integer, Integer, Traverser<Tuple2<Integer, Set<Integer>>>> flatMapFn =
                (state, key, value) -> {
                    // when encountering a keyed item, initialize its broadcast item list with all
                    // broadcast items seen so far
                    synchronized (allBroadcastItems) {
                        state.addAll(allBroadcastItems);
                    }
                    return Traversers.singleton(Tuple2.tuple2(key, new HashSet<>(state)));
                };
        TriFunction<Set<Integer>, Integer, Integer, Traverser<Tuple2<Integer, Set<Integer>>>> broadcastMapFn =
                (state, key, value) -> {
                    // when encountering a broadcast item, add it to the encountered list of the
                    // current key, as well as to the global encountered list
                    if (state == null) {
                        // state == key == null is guaranteed to be invoked before any actual keys,
                        // so we only have to do this once for a given broadcast item
                        synchronized (allBroadcastItems) {
                            allBroadcastItems.add(value);
                        }
                        // no keyed state to be updated
                        return Traversers.empty();
                    }

                    state.add(value);
                    return Traversers.singleton(Tuple2.tuple2(key, new HashSet<>(state)));
                };
        StreamStage<Tuple2<Integer, Set<Integer>>> outputStream =
                keyedInputStream.broadcastJoin(HashSet::new,
                        flatMapFn,
                        broadcastInputStream,
                        broadcastMapFn).setLocalParallelism(4);

        // Then
        outputStream.writeTo(sink);
        execute();
        // the exact number of output items is nondeterministic depending on the encounter order of
        // keyed versus broadcast items, but the final output item for each key should contain all
        // of the broadcast items
        Map<Integer, Set<Integer>> broadcastItemsByKey =
                sinkStreamOf(Tuple2.class).collect(Collectors.toMap(t -> (Integer) t.f0(),
                t -> (Set<Integer>) t.f1(), (s1, s2) -> s2, TreeMap::new));
        assertEquals("number of distinct keys in output should equal number in input",
                keyedInput.size(), broadcastItemsByKey.size());
        broadcastItemsByKey.forEach((k, v) -> assertEquals("number of broadcast items for key "
                + k + " should equal total number of broadcast items", broadcastInput.size(),
                v.size()));
    }
}
