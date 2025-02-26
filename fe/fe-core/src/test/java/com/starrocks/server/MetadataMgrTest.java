// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.starrocks.external.hive.HiveMetaStoreThriftClient;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MetadataMgrTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createTbl = "create table db1.tbl1(k1 varchar(32), catalog varchar(32), external varchar(32), k4 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        starRocksAssert.withTable(createTbl);
    }

    @Test
    public void testListDbNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws TException {
        new Expectations() {
            {
                metaStoreThriftClient.getAllDatabases();
                result = Lists.newArrayList("db2");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> internalListDbs = metadataMgr.listDbNames("default_catalog");
        Assert.assertTrue(internalListDbs.contains("default_cluster:db1"));

        List<String> externalListDbs = metadataMgr.listDbNames("hive_catalog");
        Assert.assertTrue(externalListDbs.contains("db2"));
    }

    @Test
    public void testListTblNames(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws TException {
        new Expectations() {
            {
                metaStoreThriftClient.getAllTables("db2");
                result = Lists.newArrayList("tbl2");
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        List<String> internalTables = metadataMgr.listTableNames("default_catalog", "default_cluster:db1");
        Assert.assertTrue(internalTables.contains("tbl1"));
        Assert.assertTrue(metadataMgr.listTableNames("default_catalog", "default_cluster:db2").isEmpty());

        List<String> externalTables = metadataMgr.listTableNames("hive_catalog", "db2");
        Assert.assertTrue(externalTables.contains("tbl2"));
        Assert.assertTrue(metadataMgr.listTableNames("hive_catalog", "db3").isEmpty());
    }

    @Test
    public void testGetDb(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws TException {
        new Expectations() {
            {
                metaStoreThriftClient.getDatabase("db2");
                result = new Database("db2", "", "", null);
                minTimes = 0;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        com.starrocks.catalog.Database database = metadataMgr.getDb("default_catalog", "default_cluster:db1");
        Assert.assertNotNull(database);
        Assert.assertNull(metadataMgr.getDb("default_catalog", "db1"));

        com.starrocks.catalog.Database database1 = metadataMgr.getDb("hive_catalog", "db2");
        Assert.assertNotNull(database1);

        com.starrocks.catalog.Database database2 = metadataMgr.getDb("hive_catalog", "db3");
        Assert.assertNull(database2);

        Assert.assertNull(metadataMgr.getDb("not_exist_catalog", "xxx"));
    }

    @Test
    public void testGetTable(@Mocked HiveMetaStoreThriftClient metaStoreThriftClient) throws TException {
        List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "BIGINT", ""));
        List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
        String hdfsPath = "hdfs://127.0.0.1:10000/hive";
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(unPartKeys);
        sd.setLocation(hdfsPath);
        Table msTable1 = new Table();
        msTable1.setDbName("hive_db");
        msTable1.setTableName("hive_table");
        msTable1.setPartitionKeys(partKeys);
        msTable1.setSd(sd);
        msTable1.setTableType("MANAGED_TABLE");
        new Expectations() {
            {
                metaStoreThriftClient.getTable("hive_db", "hive_table");
                result = msTable1;
            }
        };

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        com.starrocks.catalog.Table internalTable = metadataMgr.getTable("default_catalog", "default_cluster:db1", "tbl1");
        Assert.assertNotNull(internalTable);
        Assert.assertNull(metadataMgr.getTable("default_catalog", "not_exist_db", "xxx"));
        Assert.assertNull(metadataMgr.getTable("default_catalog", "db1", "not_exist_table"));

        com.starrocks.catalog.Table tbl1 = metadataMgr.getTable("hive_catalog", "hive_db", "hive_table");
        Assert.assertNotNull(tbl1);

        com.starrocks.catalog.Table tbl2 = metadataMgr.getTable("not_exist_catalog", "xxx", "xxx");
        Assert.assertNull(tbl2);

        com.starrocks.catalog.Table tbl3 = metadataMgr.getTable("hive_catalog", "not_exist_db", "xxx");
        Assert.assertNull(tbl3);

        com.starrocks.catalog.Table tbl4 = metadataMgr.getTable("hive_catalog", "hive_db", "not_exist_tbl");
        Assert.assertNull(tbl4);
    }
}
