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

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class NewConsumerExample {
  private static final String CLIENT_ID_CONFIG = "client.id";
  private static final String GROUP_ID_CONFIG = "group.id";
  private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  private static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
  private static final String KEY_DESERILIZER_CONFIG = "key.deserializer";
  private static final String VALUE_DESERILIZER_CONFIG = "value.deserializer";

  private ConsumeLoop consumeLoop;
  private Thread consumeLoopThread;

  public NewConsumerExample(int numProcessors) {
    Properties config = createConfig();
    List<String> topics = Collections.singletonList("test");
    consumeLoop = new ConsumeLoop(config, topics, numProcessors);
    consumeLoopThread = new Thread(consumeLoop);
    consumeLoopThread.start();
  }

  private Properties createConfig()  {
    Properties config = new Properties();
    config.put(CLIENT_ID_CONFIG, "example_client");
    config.put(GROUP_ID_CONFIG, "example");
    config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ENABLE_AUTO_COMMIT_CONFIG, false);
    config.put(KEY_DESERILIZER_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(VALUE_DESERILIZER_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return config;
  }

  public void shutdown() throws InterruptedException {
    consumeLoop.shutdown();
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("The only allowed argument is numProcessors.");
      System.exit(-1);
    }

    int numProcessors = Integer.parseInt(args[0]);
    if (numProcessors <= 0) {
      System.out.println("numProcessors should be positive.");
      System.exit(-1);
    }

    final NewConsumerExample example = new NewConsumerExample(numProcessors);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          example.shutdown();
        } catch (InterruptedException e) {
          //
        }
      }
    });
  }
}
