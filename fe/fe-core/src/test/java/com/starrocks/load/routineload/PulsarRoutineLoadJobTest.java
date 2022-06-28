// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/PulsarRoutineLoadJobTest.java

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

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.*;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.qe.ConnectContext;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class PulsarRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(PulsarRoutineLoadJobTest.class);

    private String jobName = "pulsar_routine_load";
    private String dbName = "test";
    private LabelName labelName = new LabelName(dbName, jobName);
    private String tableNameString = "routine_load_test_5";
    private String topicName = "sr_dev";
    private String serverAddress = "pulsar://42.194.172.174:6651/";
    private String pulsarPartitionString = "1,2";

    private PartitionNames partitionNames;

    private ColumnSeparator columnSeparator = new ColumnSeparator(",");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNames(false, partitionNameList);
    }

    @Test
    public void testBeNumMin(@Mocked Catalog catalog,
                             @Mocked SystemInfoService systemInfoService,
                             @Mocked Database database,
                             @Mocked RoutineLoadDesc routineLoadDesc) throws MetaNotFoundException {
        List<Integer> partitionList1 = Lists.newArrayList(1, 2);
        List<Integer> partitionList2 = Lists.newArrayList(1, 2, 3);
        List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3, 4);
        List<Integer> partitionList4 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        List<Long> beIds1 = Lists.newArrayList(1L);
        List<Long> beIds2 = Lists.newArrayList(1L, 2L, 3L, 4L);

        String clusterName1 = "default1";
        String clusterName2 = "default2";

        new Expectations() {
            {
                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
                systemInfoService.getClusterBackendIds(clusterName1, true);
                minTimes = 0;
                result = beIds1;
                systemInfoService.getClusterBackendIds(clusterName2, true);
                result = beIds2;
                minTimes = 0;
            }
        };

        // 2 partitions, 1 be
        RoutineLoadJob routineLoadJob =
                new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", clusterName1, 1L,
                        1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentPulsarPartitions", partitionList1);
        Assert.assertEquals(1, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 3 partitions, 4 be
        routineLoadJob = new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", clusterName2, 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentPulsarPartitions", partitionList2);
        Assert.assertEquals(3, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 4 partitions, 4 be
        routineLoadJob = new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", clusterName2, 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentPulsarPartitions", partitionList3);
        Assert.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());

        // 7 partitions, 4 be
        routineLoadJob = new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", clusterName2, 1L,
                1L, "127.0.0.1:9020", "topic1");
        Deencapsulation.setField(routineLoadJob, "currentPulsarPartitions", partitionList4);
        Assert.assertEquals(4, routineLoadJob.calculateCurrentConcurrentTaskNum());
    }

    @Test
    public void testDivideRoutineLoadJob(@Injectable RoutineLoadManager routineLoadManager,
                                         @Mocked RoutineLoadDesc routineLoadDesc)
            throws UserException {

        Catalog catalog = Deencapsulation.newInstance(Catalog.class);

        RoutineLoadJob routineLoadJob =
                new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", "default", 1L,
                        1L, "127.0.0.1:9020", "topic1");

        new Expectations(catalog) {
            {
                catalog.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.setField(catalog, "routineLoadTaskScheduler", routineLoadTaskScheduler);

        Deencapsulation.setField(routineLoadJob, "currentPulsarPartitions", Arrays.asList(1, 4, 6));

        routineLoadJob.divideRoutineLoadJob(2);

        // todo(ml): assert
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
        Assert.assertEquals(2, routineLoadTaskInfoList.size());
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            PulsarTaskInfo pulsarTaskInfo = (PulsarTaskInfo) routineLoadTaskInfo;
            Assert.assertEquals(false, pulsarTaskInfo.isRunning());
            if (pulsarTaskInfo.getPartitions().size() == 2) {
                Assert.assertTrue(pulsarTaskInfo.getPartitions().contains(1));
                Assert.assertTrue(pulsarTaskInfo.getPartitions().contains(6));
            } else if (pulsarTaskInfo.getPartitions().size() == 1) {
                Assert.assertTrue(pulsarTaskInfo.getPartitions().contains(4));
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void testProcessTimeOutTasks(@Injectable GlobalTransactionMgr globalTransactionMgr,
                                        @Injectable RoutineLoadManager routineLoadManager) {
        Catalog catalog = Deencapsulation.newInstance(Catalog.class);

        RoutineLoadJob routineLoadJob =
                new PulsarRoutineLoadJob(1L, "pulsar_routine_load_job", "default", 1L,
                        1L, "127.0.0.1:9020", "topic1");
        long maxBatchIntervalS = 10;
        new Expectations() {
            {
                catalog.getRoutineLoadManager();
                minTimes = 0;
                result = routineLoadManager;
            }
        };

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        Map<Integer, Long> partitionIdsToOffset = Maps.newHashMap();
        partitionIdsToOffset.put(100, 0L);
        PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(new UUID(1, 1), 1L, "default_cluster",
                maxBatchIntervalS * 2 * 1000, System.currentTimeMillis(), partitionIdsToOffset);
        pulsarTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis() - maxBatchIntervalS * 2 * 1000 - 1);
        routineLoadTaskInfoList.add(pulsarTaskInfo);

        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);

        routineLoadJob.processTimeoutTasks();
        new Verifications() {
            {
                List<RoutineLoadTaskInfo> idToRoutineLoadTask =
                        Deencapsulation.getField(routineLoadJob, "routineLoadTaskInfoList");
                Assert.assertNotEquals("1", idToRoutineLoadTask.get(0).getId());
                Assert.assertEquals(1, idToRoutineLoadTask.size());
            }
        };
    }

    @Test
    public void testFromCreateStmtWithErrorTable(@Mocked Catalog catalog,
                                                 @Injectable Database database) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                database.getTable(tableNameString);
                minTimes = 0;
                result = null;
            }
        };

        try {
            PulsarRoutineLoadJob pulsarRoutineLoadJob = PulsarRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmt(@Mocked Catalog catalog,
                                   @Injectable Database database,
                                   @Injectable OlapTable table) throws UserException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        List<Pair<Integer, Long>> partitionIdToOffset = Lists.newArrayList();
        for (String s : pulsarPartitionString.split(",")) {
            partitionIdToOffset.add(new Pair<>(Integer.valueOf(s), 0L));
        }
        Deencapsulation.setField(createRoutineLoadStmt, "pulsarPartitionOffsets", partitionIdToOffset);
        Deencapsulation.setField(createRoutineLoadStmt, "pulsarServerUrl", serverAddress);
        Deencapsulation.setField(createRoutineLoadStmt, "pulsarTopic", topicName);
        long dbId = 1l;
        long tableId = 2L;

        new Expectations() {
            {
                database.getTable(tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.getType();
                minTimes = 0;
                result = Table.TableType.OLAP;
            }
        };

        new MockUp<PulsarUtil>() {
            @Mock
            public List<Integer> getAllPulsarPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws UserException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        PulsarRoutineLoadJob pulsarRoutineLoadJob = PulsarRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assert.assertEquals(jobName, pulsarRoutineLoadJob.getName());
        Assert.assertEquals(dbId, pulsarRoutineLoadJob.getDbId());
        Assert.assertEquals(tableId, pulsarRoutineLoadJob.getTableId());
        Assert.assertEquals(serverAddress, Deencapsulation.getField(pulsarRoutineLoadJob, "serverUrl"));
        Assert.assertEquals(topicName, Deencapsulation.getField(pulsarRoutineLoadJob, "topic"));
        List<Integer> pulsarPartitionResult = Deencapsulation.getField(pulsarRoutineLoadJob, "customPulsarPartitions");
        Assert.assertEquals(pulsarPartitionString, Joiner.on(",").join(pulsarPartitionResult));
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        loadPropertyList.add(partitionNames);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.PULSAR.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.PULSAR_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.PULSAR_SERVICE_URL_PROPERTY, serverAddress);
        customProperties.put(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY, pulsarPartitionString);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }
}
