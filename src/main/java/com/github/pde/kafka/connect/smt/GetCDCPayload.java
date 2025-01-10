/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package com.github.pde.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class GetCDCPayload<R extends ConnectRecord<R>> implements Transformation<R> {
    protected Logger logger = LoggerFactory.getLogger("GetCDCPayload");

    public static final String OVERVIEW_DOC =
            "Get final payload from CDC message";

    private interface ConfigName {
        String KEY_FIELD_NAME = "key.field.name";
        String SKIP_FIELD_NAMES = "skip.field.names";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ConfigName.KEY_FIELD_NAME, ConfigDef.Type.STRING,
                    "uuid", ConfigDef.Importance.HIGH,
                    "Field name for message key")
            .define(
                    ConfigName.SKIP_FIELD_NAMES, ConfigDef.Type.STRING,
                    "", ConfigDef.Importance.HIGH,
                    "Comma separated fields to skip from payload")
            ;

    private static final String PURPOSE = "Extracting payload from CDC message";

    private String keyFieldName;
    private String[] skipFieldNames;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        keyFieldName = config.getString(ConfigName.KEY_FIELD_NAME);
        skipFieldNames = config.getString(ConfigName.SKIP_FIELD_NAMES).split(",");

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        logger.info("apply: record== {}", record);

        if (operatingSchema(record) == null) {
            logger.info("GetCDCPayload: applySchemaless");
            return applySchemaless(record);
        } else {
            logger.info("GetCDCPayload: applyWithSchema");
            // @todo fix it.
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        Map<String, Object> updatedValue = new HashMap<>(value);

        Object key = record.key();
        if (updatedValue.containsKey("meta") && updatedValue.containsKey("payload")) {
            final HashMap meta = (HashMap) updatedValue.get("meta");
            final HashMap payload = (HashMap) updatedValue.get("payload");
            final String operation = (String) meta.get("operation");
            logger.info("meta: operation== {}", operation);
            logger.info("meta: skipFieldNames== {}", skipFieldNames);

            if (Objects.equals(operation, "deleted")) {
                final HashMap before = (HashMap) payload.get("before");
                final HashMap beforeProperties = (HashMap) before.get("properties");
                updatedValue = null;
                key = beforeProperties.getOrDefault(keyFieldName, key);

            } else {
                final HashMap after = (HashMap) payload.get("after");
                final HashMap afterProperties = (HashMap) after.get("properties");
                afterProperties.keySet().removeAll(Arrays.asList(skipFieldNames));
                updatedValue = afterProperties;
                key = updatedValue.getOrDefault(keyFieldName, key);
            }
        }


        logger.info("applySchemaless: key== {}", key);
        logger.info("applySchemaless: updatedValue== {}", updatedValue);
        final R x = newRecord(record, null, updatedValue, key);
        logger.info("applySchemaless: newRecord== {}", x);
        return x;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(keyFieldName, getRandomUuid());

        return newRecord(record, updatedSchema, updatedValue, record.key());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private String getRandomUuid() {
        return UUID.randomUUID().toString();
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(keyFieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Object operation);




    public static class Key<R extends ConnectRecord<R>> extends GetCDCPayload<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Object key) {
            logger.info("Key: with key: {} on newRecord== {}", key, updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, key, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends GetCDCPayload<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Object key) {
            logger.info("Value: with key: {} on newRecord== {}", key, updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), key, updatedSchema, updatedValue, record.timestamp());
        }

    }
}


