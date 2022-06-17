// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/KafkaUtil.java

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

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.PPulsarOffsetProxyRequest;
import com.starrocks.proto.PPulsarOffsetProxyResult;
import com.starrocks.proto.PPulsarLoadInfo;
import com.starrocks.proto.PPulsarMetaProxyRequest;
import com.starrocks.proto.PPulsarOffsetBatchProxyRequest;
import com.starrocks.proto.PStringPair;
import com.starrocks.rpc.BackendServiceProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);

    private static final PulsarUtil.ProxyAPI proxyApi = new PulsarUtil.ProxyAPI();

    public static List<Integer> getAllPulsarPartitions(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties) throws UserException {
        return proxyApi.getAllPulsarPartitions(brokerList, topic, properties);
    }

    // latest offset is (the latest existing message offset + 1)
    public static Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions) throws UserException {
        return proxyApi.getLatestOffsets(brokerList, topic, properties, partitions);
    }

    public static Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                         ImmutableMap<String, String> properties,
                                                         List<Integer> partitions) throws UserException {
        return proxyApi.getBeginningOffsets(brokerList, topic, properties, partitions);
    }

    public static List<PPulsarOffsetProxyResult> getBatchOffsets(List<PPulsarOffsetProxyRequest> requests)
            throws UserException {
        return proxyApi.getBatchOffsets(requests);
    }

    public static PPulsarLoadInfo genPPulsarLoadInfo(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties) {
        PPulsarLoadInfo pulsarLoadInfo = new PPulsarLoadInfo();
        pulsarLoadInfo.brokers = brokerList;
        pulsarLoadInfo.topic = topic;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PStringPair pair = new PStringPair();
            pair.key = entry.getKey();
            pair.val = entry.getValue();
            if (pulsarLoadInfo.properties == null) {
                pulsarLoadInfo.properties = Lists.newArrayList();
            }
            pulsarLoadInfo.properties.add(pair);
        }
        return pulsarLoadInfo;
    }

    static class ProxyAPI {
        public List<Integer> getAllPulsarPartitions(String brokerList, String topic,
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            PPulsarMetaProxyRequest metaRequest = new PPulsarMetaProxyRequest();
            metaRequest.pulsarInfo = genPPulsarLoadInfo(brokerList, topic, convertedCustomProperties);
            PProxyRequest request = new PProxyRequest();
            request.pulsarMetaRequest = metaRequest;

            PProxyResult result = sendProxyRequest(request);
            return result.pulsarMetaResult.partitionIds;
        }

        public Map<Integer, Long> getLatestOffsets(String brokerList, String topic,
                                                   ImmutableMap<String, String> properties,
                                                   List<Integer> partitions) throws UserException {
            return getOffsets(brokerList, topic, properties, partitions, true);
        }

        public Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                      ImmutableMap<String, String> properties,
                                                      List<Integer> partitions) throws UserException {
            return getOffsets(brokerList, topic, properties, partitions, false);
        }

        public Map<Integer, Long> getOffsets(String brokerList, String topic,
                                             ImmutableMap<String, String> properties,
                                             List<Integer> partitions, boolean isLatest) throws UserException {
            // create request
            PPulsarOffsetProxyRequest offsetRequest = new PPulsarOffsetProxyRequest();
            offsetRequest.pulsarInfo = genPPulsarLoadInfo(brokerList, topic, properties);
            offsetRequest.partitionIds = partitions;
            PProxyRequest request = new PProxyRequest();
            request.pulsarOffsetRequest = offsetRequest;

            // send request
            PProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<Integer, Long> partitionOffsets = Maps.newHashMapWithExpectedSize(partitions.size());
            List<Long> offsets;
            if (isLatest) {
                offsets = result.pulsarOffsetResult.latestOffsets;
            } else {
                offsets = result.pulsarOffsetResult.beginningOffsets;
            }
            for (int i = 0; i < result.pulsarOffsetResult.partitionIds.size(); i++) {
                partitionOffsets.put(result.pulsarOffsetResult.partitionIds.get(i), offsets.get(i));
            }
            return partitionOffsets;
        }

        public List<PPulsarOffsetProxyResult> getBatchOffsets(List<PPulsarOffsetProxyRequest> requests)
                throws UserException {
            // create request
            PProxyRequest pProxyRequest = new PProxyRequest();
            PPulsarOffsetBatchProxyRequest pPulsarOffsetBatchProxyRequest = new PPulsarOffsetBatchProxyRequest();
            pPulsarOffsetBatchProxyRequest.requests = requests;
            pProxyRequest.pulsarOffsetBatchRequest = pPulsarOffsetBatchProxyRequest;

            // send request
            PProxyResult result = sendProxyRequest(pProxyRequest);

            return result.pulsarOffsetBatchResult.results;
        }

        private PProxyResult sendProxyRequest(PProxyRequest request) throws UserException {
            TNetworkAddress address = new TNetworkAddress();
            try {
                List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
                if (backendIds.isEmpty()) {
                    throw new LoadException("Failed to send proxy request. No alive backends");
                }
                Collections.shuffle(backendIds);
                Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                Future<PProxyResult> future = BackendServiceProxy.getInstance().getInfo(address, request);
                PProxyResult result = future.get(Config.routine_load_pulsar_timeout_second, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                if (code != TStatusCode.OK) {
                    LOG.warn("failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                    throw new UserException(
                            "failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                } else {
                    return result;
                }
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

