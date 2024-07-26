/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samzaapp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class WordCount implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "sample-text";
  private static final String OUTPUT_STREAM_ID = "word-count-output";

  private static final int TARGET_WORD_COUNT = 1000000; // Number of words to process before stopping
  private static final AtomicInteger wordCount = new AtomicInteger(0);
  private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

  @Override
  public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<KV<String, String>> inputDescriptor = kafkaSystemDescriptor.getInputDescriptor(
        INPUT_STREAM_ID,
        KVSerde.of(new StringSerde(), new StringSerde()));
    KafkaOutputDescriptor<KV<String, String>> outputDescriptor = kafkaSystemDescriptor.getOutputDescriptor(
        OUTPUT_STREAM_ID,
        KVSerde.of(new StringSerde(), new StringSerde()));

    MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);

    
    lines
        .map(kv -> kv.value)
        .flatMap(s -> Arrays.asList(s.split("\\W+")))
        .filter(word -> {
          if (wordCount.incrementAndGet() >= TARGET_WORD_COUNT) {
            shutdownLatch.countDown();
            return false;
          }
          return true;
        })
        .window(Windows.keyedSessionWindow(
          w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
          new StringSerde(), new IntegerSerde()), "count")
        .map(windowPane -> KV.of(windowPane.getKey().getKey(),
          windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
        .sendTo(counts);
  }

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);
    LocalApplicationRunner runner = new LocalApplicationRunner(new WordCount(), config);

    runner.run();
    long startTime = System.nanoTime();
    try {
      shutdownLatch.await();
      runner.kill();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long endTime = System.nanoTime();
    double execTimeSec = (double)(endTime - startTime) / 1000000000.0;
    System.out.println("Count " + TARGET_WORD_COUNT + " in " + execTimeSec + " sec");
  }
}