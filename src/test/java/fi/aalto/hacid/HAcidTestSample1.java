/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fi.aalto.hacid;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import static org.junit.Assert.*;

/**
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class HAcidTestSample1 {

    protected HAcidClient client;
    protected Configuration conf;
    protected HAcidTable sampleUserTable;

    protected void createSample() throws Exception {
        conf = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(conf);
        assertTrue("HBase site must have no tables.", admin.listTables() == null || admin.listTables().length == 0);

        // Makes HAcid tables
        client = new HAcidClient(conf);
        assertTrue(admin.tableExists(Schema.TABLE_TIMESTAMP_LOG));

        // Makes a sample user table
        HTableDescriptor descSampleUserTable = new HTableDescriptor("people");
        HColumnDescriptor infoFamily = new HColumnDescriptor("info");
        descSampleUserTable.addFamily(infoFamily);
        admin.createTable(descSampleUserTable);

        // Populate user table:
        HTable userTable = new HTable(conf, "people");

        Put put1 = new Put(Bytes.toBytes("person1"));
        put1.add(
            Bytes.toBytes("info"),
            Bytes.toBytes("name"),
            Bytes.toBytes("hotloo")
        );
        userTable.put(put1);

        Put put2 = new Put(Bytes.toBytes("person2"));
        put2.add(
            Bytes.toBytes("info"),
            Bytes.toBytes("name"),
            Bytes.toBytes("andre")
        );
        userTable.put(put2);

        Put put3 = new Put(Bytes.toBytes("person3"));
        put3.add(
            Bytes.toBytes("info"),
            Bytes.toBytes("name"),
            Bytes.toBytes("daniel")
        );
        userTable.put(put3);

        // Check if user table is prepared
        HAcidClient.prepareUserTable(userTable);
        assertTrue(admin.getTableDescriptor(userTable.getTableName()).hasFamily(Schema.FAMILY_HACID));
        sampleUserTable = new HAcidTable(conf, "people");
        Scan scan = new Scan();
        ResultScanner scanner = sampleUserTable.htable.getScanner(scan);
        Result result;
        while((result = scanner.next()) != null) {
            assertEquals("committed-at in user table should be the initial timestamp",
                Schema.TIMESTAMP_INITIAL_LONG,
                Bytes.toLong(
                    result
                    .getColumnLatest(
                        Schema.FAMILY_HACID,
                        Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                    ).getValue()
                )
            );
            assertEquals("cells in user table should have the initial timestamp",
                Schema.TIMESTAMP_INITIAL_LONG,
                result
                .getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                ).getTimestamp()
            );
            assertEquals("cells in user table should have the initial timestamp",
                Schema.TIMESTAMP_INITIAL_LONG,
                result
                .getColumnLatest(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name")
                ).getTimestamp()
            );
        }
        scanner.close();
    }

    protected void destroySample() throws Exception {
        if(conf == null) {
            conf = HBaseConfiguration.create();
//            conf.addResource("/Users/liyinhgqw/w/workspace/hbase/conf/hbase-site.xml");
        }

        HBaseAdmin admin = new HBaseAdmin(conf);

        if(client == null) {
            client = new HAcidClient(conf);
        }
        if(sampleUserTable == null) {
            sampleUserTable = new HAcidTable(conf, "people");
        }

        // Closes the tables
        client.close();
        sampleUserTable.close();

        // Deletes all used tables
        admin.disableTable("people");
        admin.deleteTable("people");
        HAcidClient.uninstall(conf);
        assertTrue("HBase site must have no tables.",
            admin.listTables() == null || admin.listTables().length == 0
        );
    }

}
