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

import java.util.LinkedList;
import java.util.Collection;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class _05_PurgeTest {

    static Logger LOG = Logger.getLogger(_05_PurgeTest.class.getName());
    private static HAcidTestSample1 testSample;

    public _05_PurgeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        if(testSample == null) {
            testSample = new HAcidTestSample1();
        }
        testSample.createSample();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if(testSample == null) {
            testSample = new HAcidTestSample1();
        }
        testSample.destroySample();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testNormalTransaction() throws Exception {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
        HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person3"));
        pput1.add("info","name","dos santos");
        txn1.put(pput1);
        txn1.commit();

        // check
        Get g1 = new Get(Bytes.toBytes("person3"));
        Result r1 = testSample.sampleUserTable.get(g1);
        assertEquals("content in user table modified correctly",
                "dos santos",
                Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
        HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person3"));
        pput2.add("info","name","desmond");
        txn2.put(pput2);
        txn2.commit();

        // check
        Get g2 = new Get(Bytes.toBytes("person3"));
        Result r2 = testSample.sampleUserTable.get(g2);
        assertEquals("content in user table modified correctly",
                "desmond",
                Bytes.toString(r2.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );

        Thread.sleep(5000);
        g2 = new Get(Bytes.toBytes("person3"));
        g2.setMaxVersions();
        g2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        r2 = testSample.sampleUserTable.get(g2);

        assertEquals("ts count should be 1", 1, r2.getColumn(Bytes.toBytes("info"), Bytes.toBytes("name")).size());

        Result log1 = testSample.client.getTimestampData(txn1.getEndTimestamp());
        assertFalse(log1.containsColumn(Schema.FAMILY_WRITESET, Bytes.toBytes("people:person3")));
        Result log2 = testSample.client.getTimestampData(txn2.getEndTimestamp());
        assertTrue(log2.containsColumn(Schema.FAMILY_WRITESET, Bytes.toBytes("people:person3")));

    }
}