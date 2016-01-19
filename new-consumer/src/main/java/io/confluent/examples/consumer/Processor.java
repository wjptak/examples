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

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Processor<K, V> implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(Processor.class);
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private int id;
  private final Queue<ConsumerRecord<K,V>> queue;
  private volatile boolean processFinished;
  private State<K, V> state;

  public Processor(int id, Queue<ConsumerRecord<K, V>> queue, State<K,V> state) {
    this.id = id;
    this.queue = queue;
    this.state = state;
    processFinished = true;
  }

  @Override
  public void run() {
    try {
      while (!shutdown.get()) {
        ConsumerRecord<K, V> record;
        synchronized (queue) {
          while (queue.isEmpty()) {
            try {
              queue.wait();
            } catch (InterruptedException e) {
              // ignore
            }
          }
          record = queue.poll();
          log.info("Pulled " + record + " from queue to be processed by Processor " + id);
          processFinished = false;
        }
        process(record, state);
        getResult();
        processFinished = true;
      }
    } finally {
      shutdownLatch.countDown();
    }
  }

  public abstract void process(ConsumerRecord<K, V> record, State<K, V> state);

  public abstract void getResult();

  public void shutdown() throws InterruptedException {
    shutdown.set(true);
    shutdownLatch.countDown();
  }

  public boolean isProcessFinished() {
    return processFinished;
  }

  public int getId() {
    return id;
  }
}
