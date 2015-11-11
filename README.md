<img src="https://github.com/yahoo/omid/blob/master/doc/images/omid-logo.png" width="200">

The Omid project provides transactional support for key-value stores using Snapshot Isolation. Omid stands for Optimistically transactional Management in Datasources. HBase is the only datastore currently supported, though adaption to any datastore that provides multiple versions per cell should be straightforward.

There are 3 components in OMID;
 * The Transaction Status Oracle (TSO), which assigns transaction timestamps and resolves conflicts between transactions.
 * The commit table which stores a mapping from start timestamp to commit timestamp
 * Shadow cells, which are written alongside data cells in the datastore to allow client to resolve reads without consulting the commit table.

To start a transaction, the client requests a start timestamp from the TSO. It then writes any data cells it wishes to write (i.e. the write set) with the start timestamp as the version of the data cell. To commit the transaction, the client sends the write set to the TSO, which will check for conflicts. If there is no conflict, the TSO assigns a commit timestamp to the transaction, and writes the mapping of the start timestamp to commit timestamp to the commit table. Once the mapping has been persisted, the commit timestamp is returned to the client. On receiving the commit timestamp from the server, the client updates the shadow cells for the write set and clears the transaction from the commit table.

To read a cell transactionally, the client first checks if the cell has a shadow cell. If the commit timestamp of the shadow cell is lower than the start timestamp of the reading transaction, then the client can "see" the cell. If there is no shadow cell, the commit table is consulted to find the commit timestamp of the cell. If the commit timestamp does not exist in the commit table, then the cell is assumed to below to an aborted transaction.

There are currently two implementations of the commit table, a HBase implementation and an In-Memory implementation. The In-Memory implementation gives no persistence guarantee and is only useful for benchmarking the TSO. 

Quickstart
----------

[![Join the chat at https://gitter.im/yahoo/omid](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/yahoo/omid?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Clone the repository and build the TSO package:

      $ git clone git@github.com:yahoo/omid.git
      $ cd omid
      $ mvn clean install assembly:single

This will generate a binary package containing all dependencies for the TSO in tso-server/target/tso-server-<VERSION>-bin.tar.gz.

Extract this package on your server

      $ tar zxvf tso-server-<VERSION>-bin.tar.gz
      $ cd tso-server-<VERSION>

Ensure that the setting for hbase.zookeeper.quorum in conf/hbase-site.xml points to your zookeeper instance, and create the commit and timestamp tables.
      
      $ bin/omid.sh create-hbase-commit-table -numSplits 16
      $ bin/omid.sh create-hbase-timestamp-table

Then start the TSO.

      $ bin/omid.sh tso

By default the tso listens on port 54758.

HBase Client usage
------------------

The client interfaces for OMID2 are _TTable_ and _TransactionManager_. A builder is provided in the
_HBaseTransactionManager_ class in order to get the TransactionManager interface, which is used for creating and
committing transactions. TTable can be used for putting, getting and scanning entries in a HBase table. TTable's
interface is similar to the standard _HTableInterface_, and only requires passing the transactional context as a
first parameter in the transactional aware methods (e.g. _put(Transaction tx, Put put)_)
_These interfaces will likely change slightly in future._

To run this example, make sure _core-site.xml_ and _hbase-site.xml_ for your HBase cluster are present in
your classpath. You will need to set _tso.host_ and _tso.port_ appropriately. Also, you will need to create a hbase
table "EXAMPLE_TABLE", with column family "EXAMPLE_CF", and with TTL disabled and maxVersions set to Integer.MAX_VALUE.
This example assumes non-secure communication with HBase. If your HBase cluster is secured with Kerberos, you will
need to use the `UserGroupInformation` API to log in securely.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;


public class Example {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        TransactionManager tm = HBaseTransactionManager.newBuilder()
                                                       .withConfiguration(conf)
                                                       .build();

        TTable tt = new TTable(conf, "EXAMPLE_TABLE");
        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] family = Bytes.toBytes("EXAMPLE_CF");
        byte[] qualifier = Bytes.toBytes("foo");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        Transaction tx = tm.begin();
        Put row1 = new Put(exampleRow1);
        row1.add(family, qualifier, dataValue1);
        tt.put(tx, row1);
        Put row2 = new Put(exampleRow2);
        row2.add(family, qualifier, dataValue2);
        tt.put(tx, row2);
        tm.commit(tx);

        tt.close();
        tm.close();
    }
}
```

HBase Co-processor Usage
------------------------
Omid includes an HBase co-processor that operates during compactions (minor and major) and performs
data cleanup. Specifically, it does the following:
 * Cleans up garbage data from aborted transactions
 * Purges deleted cells. Omid deletes work by placing a special tombstone marker in cells. The compactor
   detects these and actually purges data when it is safe to do so (i.e. when there are no committable transactions
   that may read the data).
 * 'Heals' committed cells for which the writer failed to write shadow cells.

To deploy the co-processor, the co-processor jar must be placed in a location (typically on HDFS) that is accessible
by HBase region servers. The co-processor may then be enabled on a transactional table by the following steps:
 * Add a co-processor specification to the table via a "co-processor attribute". The co-processor spec may (and usually will)
   also include the name of the Omid commit table.
 * Add an "OMID_ENABLED => true" flag to any column families which the co-processor should work on.
 * Disable and re-enable the table.

Sample co-processor enabled table:
```
'omid_enabled_table', {TABLE_ATTRIBUTES =>
   {coprocessor$1 => 'hdfs:///hbase/co_processor/omid/omid2-hbase-coprocessor-2.2.11.jar|
                      com.yahoo.omid.transaction.OmidCompactor|1001|
                      omid.committable.tablename=ns:OMID_COMMIT_TABLE},
   {NAME => 'n', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'ROW', REPLICATION_SCOPE => '0',
    COMPRESSION => 'LZO', VERSIONS => '2147483647', TTL => 'FOREVER', MIN_VERSIONS => '0',
    KEEP_DELETED_CELLS => 'false', BLOCKSIZE => '65536', IN_MEMORY => 'true', BLOCKCACHE => 'true'},
   {NAME => 's', DATA_BLOCK_ENCODING => 'PREFIX', BLOOMFILTER => 'ROW', REPLICATION_SCOPE => '0',
    COMPRESSION => 'LZO', VERSIONS => '1', TTL => 'FOREVER', MIN_VERSIONS => '0',
    KEEP_DELETED_CELLS => 'false', BLOCKSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true',
    METADATA => {'OMID_ENABLED' => 'true'}}
```

Module Dependencies
-------------------
![Module dependencies](https://github.com/yahoo/omid/blob/master/doc/images/ModuleDependencies.png "Module dependencies")

License
-------
Code licensed under the Apache License Version 2.0. See LICENSE file for terms.