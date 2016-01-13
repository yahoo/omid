package com.yahoo.omid;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

public class HBaseShims {
    static public void setKeyValueSequenceId(KeyValue kv, int sequenceId) {
        kv.setMvccVersion(sequenceId);
    }

    static public Region getRegionCoprocessorRegion(
            RegionCoprocessorEnvironment env) {
        return new Region(env.getRegion());
    }

    static public void flushAllOnlineRegions(HRegionServer regionServer,
            TableName tableName) throws IOException {
        for (HRegion r : regionServer.getOnlineRegions(tableName)) {
            r.flushcache();
        }
    }
}