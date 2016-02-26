/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.examples;

import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ****************************************************************************************************
 *
 * Example code which demonstrates an atomic write into two different rows in HBase
 *
 * ****************************************************************************************************
 *
 * After building the package with 'mvn clean package' find the resulting examples-<version>-bin.tar.gz file in the
 * 'examples/target' folder. Copy it to the target host and expand with 'tar -zxvf examples-<version>-bin.tar.gz'.
 *
 * Make sure that 'hbase-site.xml' and 'core-site.xml' are either in classpath (see run.sh) or explicitly referenced via
 * command line arguments. If a secure HBase deployment is needed, use also command line arguments to specify the
 * principal (user) and keytab file.
 *
 * The example requires a user table to perform transactional read/write operations. A table is already specified in
 * the default configuration, and can be created with the following command using the 'hbase shell':
 *
 * <pre>
 * create 'MY_TX_TABLE', {NAME => 'MY_CF', VERSIONS => '2147483647', TTL => '2147483647'}
 * </pre>
 *
 * Make sure that the principal/user has RW permissions for the given table using also the 'hbase shell':
 * <pre>
 * grant '<principal/user>', 'RW', 'MY_TX_TABLE'
 * </pre>
 *
 * Alternatively, a table with a column family already created can be used by specifying the table name and column
 * family identifiers using the command line arguments (see details also in 'run.sh') If a table namespace is required,
 * specify it like this: 'namespace:table_name'
 *
 * Finally, run the example using the 'run.sh' script without arguments or specifying the necessary configuration
 * parameters.
 */
public class BasicExample {

    private static final Logger LOG = LoggerFactory.getLogger(BasicExample.class);

    public static void main(String[] args) throws Exception {

        LOG.info("Parsing command line arguments");
        Configuration exampleConfig = Configuration.parse(args);

        String userTableName = exampleConfig.userTableName;
        byte[] family = Bytes.toBytes(exampleConfig.cfName);
        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] qualifier = Bytes.toBytes("MY_Q");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        //Logging in to Secure HBase if required
        HBaseLogin.loginIfNeeded(exampleConfig);

        LOG.info("Creating HBase Transaction Manager");
        TransactionManager tm = HBaseTransactionManager.newInstance(exampleConfig.createOmidClientConfig());

        LOG.info("Creating access to Transactional Table '{}'", userTableName);
        try (TTable tt = new TTable(HBaseConfiguration.create(), userTableName)) {

            Transaction tx = tm.begin();
            LOG.info("Transaction {} STARTED", tx);

            Put row1 = new Put(exampleRow1);
            row1.add(family, qualifier, dataValue1);
            tt.put(tx, row1);
            LOG.info("Transaction {} writing value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                     tx, userTableName, Bytes.toString(exampleRow1), Bytes.toString(family),
                     Bytes.toString(qualifier), Bytes.toString(dataValue1));

            Put row2 = new Put(exampleRow2);
            row2.add(family, qualifier, dataValue2);
            tt.put(tx, row2);
            LOG.info("Transaction {} writing value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                     tx, userTableName, Bytes.toString(exampleRow2), Bytes.toString(family),
                     Bytes.toString(qualifier), Bytes.toString(dataValue2));

            tm.commit(tx);
            LOG.info("Transaction {} COMMITTED", tx);
        }

        tm.close();

    }

}
