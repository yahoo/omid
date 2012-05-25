/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.client.CommitUnsuccessfulException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;

public class TestIsolationLevels extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestIsolationLevels.class);

    @Test
    public void runTestWriteWriteConflict() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);

        tm.tryCommit(t2);

        boolean aborted = false;
        try {
            tm.tryCommit(t1);
            assertFalse("Transaction commited successfully, despite the check for write-write conflicts", IsolationLevel.checkForWriteWriteConflicts);
        } catch (CommitUnsuccessfulException e) {
            aborted = true;
            assertTrue("Transaction aborted even though check for write-write conflicts is not set", IsolationLevel.checkForWriteWriteConflicts);
        }

        if (!aborted) {
            //check the read snapshot property
            TransactionState tread = tm.beginTransaction();
            LOG.info("Transaction created" + tread);
            Get g = new Get(row).setMaxVersions(1);
            Result r = tt.get(tread, g);
            boolean isTheLastCommitRead = Bytes.equals(data1, r.getValue(fam, col));
            assertTrue("The last committed value is not visible.", isTheLastCommitRead);
        }
    }

    //This tests stresses the filter function as well
    @Test
    public void runTestReadSnapshotProperty() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");

        for (int ti = 0; ti < 14; ti++) {
            TransactionState t1 = tm.beginTransaction();
            LOG.info("Transaction created " + t1);
            Put p = new Put(row);
            p.add(fam, col, Integer.toString(ti).getBytes());
            tt.put(t1, p);
            tm.tryCommit(t1);
        }

        TransactionState tinprogress = tm.beginTransaction();
        LOG.info("Transaction created " + tinprogress);
        Put p = new Put(row);
        p.add(fam, col, Integer.toString(123).getBytes());
        tt.put(tinprogress, p);
        //do not commit this one
        //tm.tryCommit(tinprogress);

        TransactionState t14 = tm.beginTransaction();
        LOG.info("Transaction created" + t14);

        for (int ti = 15; ti < 27; ti++) {
            TransactionState t1 = tm.beginTransaction();
            LOG.info("Transaction created " + t1);
            p = new Put(row);
            p.add(fam, col, Integer.toString(ti).getBytes());
            tt.put(t1, p);
            tm.tryCommit(t1);
        }

        TransactionState taborted = tm.beginTransaction();
        LOG.info("Transaction created " + taborted);
        p = new Put(row);
        p.add(fam, col, Integer.toString(234).getBytes());
        tt.put(taborted, p);
        tm.abort(taborted);

        TransactionState t27 = tm.beginTransaction();
        LOG.info("Transaction created" + t27);

        Get g14 = new Get(row);
        Result r = tt.get(t14, g14);
        boolean isTheLastCommitRead = Bytes.equals(Integer.toString(13).getBytes(), r.getValue(fam, col));
        assertTrue("The valus of the last commit is not read.", isTheLastCommitRead);

        Get g27 = new Get(row);
        r = tt.get(t27, g27);
        isTheLastCommitRead = Bytes.equals(Integer.toString(26).getBytes(), r.getValue(fam, col));
        assertTrue("The valus of the last commit is not read.", isTheLastCommitRead);

        tm.tryCommit(t14);
        tm.tryCommit(t27);
    }

    @Test
    public void runTestReadWriteConflict() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row2 = Bytes.toBytes("test-simple2");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        //dummy put, to make the transaciton not read-only
        Put p2 = new Put(row2);
        p2.add(fam, col, data2);
        tt.put(t2, p2);
        Get g = new Get(row).setMaxVersions(1);
        Result r = tt.get(g);
        tt.get(t2, g);

        //transaction t1 commit sooner. Since t1 modifies the read set of t2 and commits during t2's lifetime, t2 must abort under read-write conflict checking.
        tm.tryCommit(t1);

        try {
            tm.tryCommit(t2);
            assertFalse("Transaction commited successfully, despite the check for read-write conflicts", IsolationLevel.checkForReadWriteConflicts);
        } catch (CommitUnsuccessfulException e) {
            assertTrue("Transaction aborted even though check for read-write conflicts is not set", IsolationLevel.checkForReadWriteConflicts);
        }
    }

    @Test
    public void runTestFakeReadWriteConflict() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row2 = Bytes.toBytes("test-simple2");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        //dummy put, to make the transaciton not read-only
        Put p2 = new Put(row2);
        p2.add(fam, col, data2);
        tt.put(t2, p2);
        Get g = new Get(row).setMaxVersions(1);
        tt.get(t2, g);

        //transaction t2 commit sooner. Since t1, which modifies the read set of t2, does not commits during t2's lifetime, t1 does not have to abort under read-write conflict checking.
        tm.tryCommit(t2);

        boolean aborted = false;
        try {
            tm.tryCommit(t1);
        } catch (CommitUnsuccessfulException e) {
            aborted = true;
        }
        assertTrue("Transaction aborted while there is no conflict", !aborted);
    }

    @Test
    public void runTestUpdate() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        //Note: the order of get and put would not matter
        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);
        Get g = new Get(row).setMaxVersions(1);
        tt.get(t1, g);

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);
        Get g2 = new Get(row).setMaxVersions(1);
        tt.get(t2, g2);

        //Since both transaction read and write (update) the row, we should abort under any isolation level
        tm.tryCommit(t2);

        boolean aborted = false;
        try {
            tm.tryCommit(t1);
        } catch (CommitUnsuccessfulException e) {
            aborted = true;
        }
        assertTrue("Transaction did aborted while there is an update conflict", aborted);
    }

}
