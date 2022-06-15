// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/RoutineLoadDataSourceProperties.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.load.routineload.LoadDataSourceType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadDataSourceProperties {
    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .build();

    private static final ImmutableSet<String> CONFIGURABLE_PROPERTIES_SET_PULSAR = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.PULSAR_OFFSETS_PROPERTY)
            .build();

    @SerializedName(value = "type")
    private String type = "";
    // origin properties, no need to persist
    private Map<String, String> properties = Maps.newHashMap();
    @SerializedName(value = "kafkaPartitionOffsets")
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
    @SerializedName(value = "pulsarPartitionOffsets")
    private List<Pair<Integer, Long>> pulsarPartitionOffsets = Lists.newArrayList();
    @SerializedName(value = "customKafkaProperties")
    private Map<String, String> customKafkaProperties = Maps.newHashMap();
    @SerializedName(value = "customPulsarProperties")
    private Map<String, String> customPulsarProperties = Maps.newHashMap();

    public RoutineLoadDataSourceProperties() {
        // empty
    }

    public RoutineLoadDataSourceProperties(String type, Map<String, String> properties) {
        this.type = type.toUpperCase();
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
        checkDataSourceProperties();
    }

    public boolean hasAnalyzedProperties() {
        return !kafkaPartitionOffsets.isEmpty() || !customKafkaProperties.isEmpty()
                || !pulsarPartitionOffsets.isEmpty() || customPulsarProperties.isEmpty();
    }

    public String getType() {
        return type;
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return kafkaPartitionOffsets;
    }

    public Map<String, String> getCustomKafkaProperties() {
        return customKafkaProperties;
    }

    public List<Pair<Integer, Long>> getPulsarPartitionOffsets() {
        return pulsarPartitionOffsets;
    }

    public Map<String, String> getCustomPulsarProperties() {
        return customPulsarProperties;
    }

    private void checkDataSourceProperties() throws AnalysisException {
        LoadDataSourceType sourceType;
        try {
            sourceType = LoadDataSourceType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + type);
        }
        switch (sourceType) {
            case KAFKA:
                checkKafkaProperties();
                break;
            case PULSAR:
                checkPulsarProperties();
            default:
                break;
        }
    }

    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).filter(
                entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check partitions
        final String kafkaPartitionsString = properties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            if (!properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Partition and offset must be specified at the same time");
            }

            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(kafkaPartitionsString, Maps.newHashMap(),
                    kafkaPartitionOffsets);
        } else {
            if (properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Missing kafka partition info");
            }
        }

        // check offset
        String kafkaOffsetsString = properties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(kafkaOffsetsString, kafkaPartitionOffsets);
        }

        // check custom properties
        CreateRoutineLoadStmt.analyzeCustomProperties(properties, customKafkaProperties);
    }

    private void checkPulsarProperties() throws AnalysisException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PROPERTIES_SET.contains(entity)).filter(
                entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid pulsar custom property");
        }

        // check partitions
        final String pulsarPartitionsString = properties.get(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY);
        if (pulsarPartitionsString != null) {
            if (!properties.containsKey(CreateRoutineLoadStmt.PULSAR_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Partition and offset must be specified at the same time");
            }

            CreateRoutineLoadStmt.analyzePulsarPartitionProperty(pulsarPartitionsString, Maps.newHashMap(),
                    pulsarPartitionOffsets);
        } else {
            if (properties.containsKey(CreateRoutineLoadStmt.PULSAR_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Missing pulsar partition info");
            }
        }

        // check offset
        String pulsarOffsetsString = properties.get(CreateRoutineLoadStmt.PULSAR_OFFSETS_PROPERTY);
        if (pulsarOffsetsString != null) {
            CreateRoutineLoadStmt.analyzePulsarOffsetProperty(pulsarOffsetsString, pulsarPartitionOffsets);
        }

        // check custom properties
        CreateRoutineLoadStmt.analyzeCustomPropertiesPulsar(properties, customPulsarProperties);
    }

    @Override
    public String toString() {
        return "RoutineLoadDataSourceProperties{" +
                "type='" + type + '\'' +
                ", properties=" + properties +
                ", kafkaPartitionOffsets=" + kafkaPartitionOffsets +
                ", pulsarPartitionOffsets=" + pulsarPartitionOffsets +
                ", customKafkaProperties=" + customKafkaProperties +
                ", customPulsarProperties=" + customPulsarProperties +
                '}';
    }
}
