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
 */
package com.mongodb.kafka.connect;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;

public class JsonStringIdToMongoDbObjectId extends PostProcessor {
  public JsonStringIdToMongoDbObjectId(final MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(final SinkDocument doc, final SinkRecord orig) {
    doc.getValueDoc()
        .ifPresent(
            vd -> {
              vd.append("_id", new BsonObjectId(new ObjectId(vd.getString("id").getValue())));
              vd.remove("id");
            });
  }
}
