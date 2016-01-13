package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class Region {
    HRegion hRegion;
    public Region(HRegion hRegion) {
        this.hRegion = hRegion;
    }
    Result get(Get g) throws IOException {
        return hRegion.get(g);
    }
    HRegionInfo getRegionInfo() {
        return hRegion.getRegionInfo();
    }
}