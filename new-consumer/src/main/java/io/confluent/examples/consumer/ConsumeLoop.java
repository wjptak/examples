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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeLoop implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(ConsumeLoop.class);
  private final KafkaConsumer<Object, Object> consumer;
  private final List<String> topics;
  private final AtomicBoolean shutdown;
  private final CountDownLatch shutdownLatch;

  private final Queue<ConsumerRecord<Object, Object>> queue;
  private final State<Object, Object> state;
  private int numProcessors;
  private Processor[] processors;
  private Thread[] processorThreads;
  private boolean committed = true;

  public ConsumeLoop(Properties config, List<String> topics, int numProcessors) {
    this.consumer = new KafkaConsumer<>(config);
    this.topics = topics;
    this.shutdown = new AtomicBoolean(false);
    this.shutdownLatch = new CountDownLatch(1);

    this.numProcessors = numProcessors;
    processors = new Processor[numProcessors];
    processorThreads = new Thread[numProcessors];

    queue = new LinkedList<>();
    state = new RecordCounterState();

    for (int i = 0; i < numProcessors; ++i) {
      processors[i] = new RecordCounter(i, queue, state);
      processorThreads[i] = new Thread(processors[i]);
    }

    for (int i = 0; i < numProcessors; ++i) {
      processorThreads[i].start();
    }
  }

  public void run() {
    try {
      consumer.subscribe(topics);
      while (!shutdown.get()) {
        if (processFinished()) {
          if (!committed) {
            log.info("Committing offset!");
            consumer.commitSync();
            committed = true;
          }

          for (TopicPartition tp : consumer.assignment()) {
            consumer.resume(tp);
          }
        }

        ConsumerRecords<Object, Object> records = consumer.poll(2000);

        if (!records.isEmpty()) {
          committed = false;
          synchronized (queue) {
            for (ConsumerRecord<Object, Object> record : records) {
              log.info("Putting " + record + " to queue for future processing");
              queue.offer(record);
            }
            queue.notifyAll();
          }

          for (TopicPartition tp : consumer.assignment()) {
            consumer.pause(tp);
          }
        }
      }
    } catch (WakeupException e) {
      // ignore
    } catch (Exception e) {
      log.error("Unexpected error", e);
    } finally {
      consumer.close();
      try {
        log.info("Shutting down processor threads");
        for (int i = 0; i < numProcessors; ++i) {
          log.info("Shutting down processor " + processors[i].getId());
          processors[i].shutdown();
        }
       } catch (InterruptedException e) {
         // ignore
      }
      shutdownLatch.countDown();
    }
  }

  private boolean processFinished() {
    boolean finished = true;
    synchronized (queue) {
      if (!queue.isEmpty()) {
        return false;
      }
      for (int i = 0; i < numProcessors; ++i) {
        finished = finished && processors[i].isProcessFinished();
      }
      return finished;
    }
  }

  public void shutdown() throws InterruptedException {
    log.info("Shutting down consumer thread");
    shutdown.set(true);
    shutdownLatch.await();
    log.info("Shutting down consumer thread finished");
  }
}
