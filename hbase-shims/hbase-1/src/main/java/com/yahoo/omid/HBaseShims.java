package com.yahoo.omid;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;

public class HBaseShims {
    static public void setKeyValueSequenceId(KeyValue kv, int sequenceId) {
        kv.setSequenceId(sequenceId);
    }

    static public Region getRegionCoprocessorRegion(
            RegionCoprocessorEnvironment env) {
        return env.getRegion();
    }

    static public void flushAllOnlineRegions(HRegionServer regionServer, TableName tableName)
            throws IOException {
        for (Region r : regionServer.getOnlineRegions(tableName)) {
            r.flush(true);
        }
    }

    static public void addFamilyToHTableDescriptor(HTableDescriptor desc,
        HColumnDescriptor column) {
      desc.addFamily(column);
    }
}
