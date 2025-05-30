/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTask;

class MongoSinkConnnectorTest {

  @Test
  @DisplayName("Should return the expected version")
  void testVersion() {
    MoostMongoSinkConnector sinkConnector = new MoostMongoSinkConnector();

    assertEquals(Versions.VERSION, sinkConnector.version());
  }

  @Test
  @DisplayName("test task class")
  void testTaskClass() {
    MoostMongoSinkConnector sinkConnector = new MoostMongoSinkConnector();

    assertEquals(MongoSinkTask.class, sinkConnector.taskClass());
  }

  @Test
  @DisplayName("test task configs")
  void testConfig() {
    MoostMongoSinkConnector sinkConnector = new MoostMongoSinkConnector();

    assertEquals(MongoSinkConfig.CONFIG, sinkConnector.config());
  }

  @Test
  @DisplayName("test task configs")
  void testTaskConfigs() {
    MoostMongoSinkConnector sinkConnector = new MoostMongoSinkConnector();
    Map<String, String> configMap =
        new HashMap<String, String>() {
          {
            put("a", "1");
            put("b", "2");
          }
        };
    sinkConnector.start(configMap);
    List<Map<String, String>> taskConfigs = sinkConnector.taskConfigs(10);

    assertEquals(10, taskConfigs.size());
    IntStream.range(0, 10).boxed().forEach(i -> assertEquals(configMap, taskConfigs.get(1)));
  }
}
