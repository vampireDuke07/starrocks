// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/TabletsProcDir.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.lake.LakeTablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 */
public class TabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES;

    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("TabletId").add("ReplicaId").add("BackendId").add("SchemaHash").add("Version")
                .add("VersionHash").add("LstSuccessVersion").add("LstSuccessVersionHash")
                .add("LstFailedVersion").add("LstFailedVersionHash").add("LstFailedTime")
                .add("DataSize").add("RowCount").add("State")
                .add("LstConsistencyCheckTime").add("CheckVersion").add("CheckVersionHash")
                .add("VersionCount").add("PathHash").add("MetaUrl").add("CompactionStatus");
        if (Config.use_staros) {
            builder.add("ShardId");
        }
        TITLE_NAMES = builder.build();
    }

    private final Database db;
    private final Partition partition;
    private final MaterializedIndex index;

    public TabletsProcDir(Database db, Partition partition, MaterializedIndex index) {
        this.db = db;
        this.partition = partition;
        this.index = index;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    public List<List<Comparable>> fetchComparableResult(long version, long backendId, Replica.ReplicaState state) {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        ImmutableMap<Long, Backend> backendMap = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();

        List<List<Comparable>> tabletInfos = new ArrayList<List<Comparable>>();
        db.readLock();
        try {
            if (partition.isUseStarOS()) {
                for (Tablet tablet : index.getTablets()) {
                    List<Comparable> tabletInfo = new ArrayList<Comparable>();
                    LakeTablet lakeTablet = (LakeTablet) tablet;
                    tabletInfo.add(lakeTablet.getId());
                    tabletInfo.add(-1); // replica id
                    tabletInfo.add(new Gson().toJson(lakeTablet.getBackendIds()));
                    tabletInfo.add(-1); // schema hash
                    tabletInfo.add(partition.getVisibleVersion());
                    tabletInfo.add(0); // version hash
                    tabletInfo.add(-1); // lst success version
                    tabletInfo.add(0); // lst success version hash
                    tabletInfo.add(-1); // lst failed version
                    tabletInfo.add(0); // lst failed version hash
                    tabletInfo.add(-1); // lst failed time
                    tabletInfo.add(lakeTablet.getDataSize(true));
                    tabletInfo.add(lakeTablet.getRowCount(0L));
                    tabletInfo.add(FeConstants.null_string); // state
                    tabletInfo.add(-1); // lst consistency check time
                    tabletInfo.add(-1); // check version
                    tabletInfo.add(0); // check version hash
                    tabletInfo.add(-1); // version count
                    tabletInfo.add(-1); // path hash
                    tabletInfo.add(FeConstants.null_string); // meta url
                    tabletInfo.add(FeConstants.null_string); // compaction status
                    if (Config.use_staros) {
                        tabletInfo.add(lakeTablet.getShardId());
                    }

                    tabletInfos.add(tabletInfo);
                }
            } else {
                // get infos
                for (Tablet tablet : index.getTablets()) {
                    LocalTablet localTablet = (LocalTablet) tablet;
                    long tabletId = tablet.getId();
                    if (localTablet.getReplicas().size() == 0) {
                        List<Comparable> tabletInfo = new ArrayList<Comparable>();
                        tabletInfo.add(tabletId);
                        tabletInfo.add(-1); // replica id
                        tabletInfo.add(-1); // backend id
                        tabletInfo.add(-1); // schema hash
                        tabletInfo.add(-1); // version
                        tabletInfo.add(0); // version hash
                        tabletInfo.add(-1); // lst success version
                        tabletInfo.add(0); // lst success version hash
                        tabletInfo.add(-1); // lst failed version
                        tabletInfo.add(0); // lst failed version hash
                        tabletInfo.add(-1); // lst failed time
                        tabletInfo.add(-1); // data size
                        tabletInfo.add(-1); // row count
                        tabletInfo.add(FeConstants.null_string); // state
                        tabletInfo.add(-1); // lst consistency check time
                        tabletInfo.add(-1); // check version
                        tabletInfo.add(0); // check version hash
                        tabletInfo.add(-1); // version count
                        tabletInfo.add(-1); // path hash
                        tabletInfo.add(FeConstants.null_string); // meta url
                        tabletInfo.add(FeConstants.null_string); // compaction status
                        if (Config.use_staros) {
                            tabletInfo.add(FeConstants.null_string); // shard id
                        }

                        tabletInfos.add(tabletInfo);
                    } else {
                        for (Replica replica : localTablet.getReplicas()) {
                            if ((version > -1 && replica.getVersion() != version)
                                    || (backendId > -1 && replica.getBackendId() != backendId)
                                    || (state != null && replica.getState() != state)) {
                                continue;
                            }
                            List<Comparable> tabletInfo = new ArrayList<Comparable>();
                            // tabletId -- replicaId -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                            tabletInfo.add(tabletId);
                            tabletInfo.add(replica.getId());
                            tabletInfo.add(replica.getBackendId());
                            tabletInfo.add(replica.getSchemaHash());
                            tabletInfo.add(replica.getVersion());
                            tabletInfo.add(0);
                            tabletInfo.add(replica.getLastSuccessVersion());
                            tabletInfo.add(0);
                            tabletInfo.add(replica.getLastFailedVersion());
                            tabletInfo.add(0);
                            tabletInfo.add(TimeUtils.longToTimeString(replica.getLastFailedTimestamp()));
                            tabletInfo.add(replica.getDataSize());
                            tabletInfo.add(replica.getRowCount());
                            tabletInfo.add(replica.getState());

                            tabletInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                            tabletInfo.add(localTablet.getCheckedVersion());
                            tabletInfo.add(0);
                            tabletInfo.add(replica.getVersionCount());
                            tabletInfo.add(replica.getPathHash());
                            Backend backend = backendMap.get(replica.getBackendId());
                            String metaUrl;
                            String compactionUrl;
                            if (backend != null) {
                                metaUrl = String.format("http://%s:%d/api/meta/header/%d/%d",
                                        backend.getHost(),
                                        backend.getHttpPort(),
                                        tabletId,
                                        replica.getSchemaHash());
                                compactionUrl = String.format(
                                        "http://%s:%d/api/compaction/show?tablet_id=%d&schema_hash=%d",
                                        backend.getHost(),
                                        backend.getHttpPort(),
                                        tabletId,
                                        replica.getSchemaHash());
                            } else {
                                metaUrl = "N/A";
                                compactionUrl = "N/A";
                            }
                            tabletInfo.add(metaUrl);
                            tabletInfo.add(compactionUrl);
                            if (Config.use_staros) {
                                tabletInfo.add(FeConstants.null_string); // shard id
                            }

                            tabletInfos.add(tabletInfo);
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
        return tabletInfos;
    }

    private List<List<Comparable>> fetchComparableResult() {
        return fetchComparableResult(-1, -1, null);
    }

    @Override
    public ProcResult fetchResult() {
        List<List<Comparable>> tabletInfos = fetchComparableResult();
        // sort by tabletId, replicaId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(tabletInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < tabletInfos.size(); i++) {
            List<Comparable> info = tabletInfos.get(i);
            List<String> row = new ArrayList<String>(info.size());
            for (int j = 0; j < info.size(); j++) {
                row.add(info.get(j).toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);

        long tabletId = -1L;
        try {
            tabletId = Long.valueOf(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        return new ReplicasProcNode(tabletId, replicas);
    }
}

