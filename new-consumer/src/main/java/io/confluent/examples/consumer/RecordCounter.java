/**
 * Copyright 2016 Confluent Inc.
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

package io.confluent.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;

public class RecordCounter extends Processor<Object, Object> {
  private static final Logger log = LoggerFactory.getLogger(Processor.class);
  private Map<String, Integer> count;

  @SuppressWarnings("unchecked")
  public RecordCounter(int id, Queue<ConsumerRecord<Object, Object>> queue, State<Object, Object> state) {
    super(id, queue, state);
    count = (Map<String, Integer>) state.getState();
  }

  @Override
  public void process(ConsumerRecord<Object, Object> record, State<Object, Object> state) {
    state.updateState(record);
  }

  @Override
  public void getResult() {
    for (Map.Entry<String, Integer> entry: count.entrySet()) {
      System.out.println("[" + entry.getKey() + ":" + entry.getValue() + "]");
    }
  }
}
