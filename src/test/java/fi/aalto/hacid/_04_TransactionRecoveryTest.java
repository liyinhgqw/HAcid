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

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
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
public class _04_TransactionRecoveryTest {

    static Logger LOG = Logger.getLogger(_04_TransactionRecoveryTest.class.getName());
    private static HAcidTestSample1 testSample;

    public _04_TransactionRecoveryTest() {
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


    /**
     * Testing whether we can recover, decide, and rollforward a txn that has
     * only an end timestamp but no decision, while doing a search for the
     * Snapshot Isolated Read Timestamp.
     *
     */
    @Test
    public void testRecoverDecideRollforwardTxnWithEndTimestampAndNoDecision() throws IOException {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person4"));
            pput1.add("info","name","glauber");
        txn1.put(pput1);

        txn1.requestEndTimestamp();

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person4"));
            pget1.addColumn("info", "name");
        Result resultGet = txn2.get(pget1);
        // In this get, while searching for the Snapshot Isolated Read Timestamp,
        // txn2 should restore txn1 and do all the remaining parts of commitment

        Result r1 = testSample.sampleUserTable.get(new Get(Bytes.toBytes("person4")));
        assertEquals("content in user table modified correctly",
            "glauber",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
        assertEquals("timestamp of user content is the start TS",
            txn1.getStartTimestamp(),
            r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is the start TS",
            txn1.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the txn's end timestamp",
            txn1.getEndTimestamp(),
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r2 = testSample.client.getTimestampData(txn1.getStartTimestamp());
        assertEquals("state of the transaction is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        txn2.commit();
    }

    /**
     * Testing whether we can recover and rollforward a txn that has a commit
     * decision but its user table data have not been committed.
     */
    @Test
    public void testRecoverRollforwardTxnWithCommitDecision() throws IOException, StateDisagreementException {
        HAcidTxn txn3 = new HAcidTxn(testSample.client);
            HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person16"));
            pput3.add("info","name","jorge");
        txn3.put(pput3);

        txn3.requestEndTimestamp();

        if(testSample.client.canCommit(txn3)) {
            testSample.client.setStateCommitted(txn3);
        }
        else {
            testSample.client.setStateAborted(txn3);
        }

        HAcidTxn txn4 = new HAcidTxn(testSample.client);
            HAcidGet pget2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person16"));
            pget2.addColumn("info", "name");
        Result resultGet2 = txn4.get(pget2);
        // In this get, while searching for the Snapshot Isolated Read Timestamp,
        // txn2 should restore txn1 and do all the remaining parts of commitment

        Result r1 = testSample.sampleUserTable.get(new Get(Bytes.toBytes("person16")));
        assertEquals("content in user table modified correctly",
            "jorge",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
        assertEquals("timestamp of user content is the start TS",
            txn3.getStartTimestamp(),
            r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is the start TS",
            txn3.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the txn's end timestamp",
            txn3.getEndTimestamp(),
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r2 = testSample.client.getTimestampData(txn3.getStartTimestamp());
        assertEquals("state of the transaction is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        txn4.commit();
    }

    /**
     * Testing whether we can recover and rollback a txn that has an abort
     * decision but its user table data have not been rollbacked.
     */
    @Test
    public void testRecoverRollbackTxnWithAbortDecision() throws IOException, StateDisagreementException {
        HAcidTxn txn5 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person17"));
            pput1.add("info","name","camarao");
            pput1.add("info","age","37");
            pput1.add("info","car","bmw");
            pput1.add("info","country","brazil");
        txn5.put(pput1);

        HAcidTxn txn6 = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person17"));
            pput2.add("info","name","rodolfo");
        txn6.put(pput2);

        txn5.commit();

        txn6.requestEndTimestamp();

        if(testSample.client.canCommit(txn6)) {
            testSample.client.setStateCommitted(txn6);
        }
        else {
            testSample.client.setStateAborted(txn6);
        }

        HAcidTxn txn7 = new HAcidTxn(testSample.client);
            HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person17"));
            pget1.addColumn("info", "name");
        Result resultGet = txn7.get(pget1);
        // In this get, while searching for the Snapshot Isolated Read Timestamp,
        // txn7 should restore txn6 and do all the remaining parts of abortion

        Result r1 = testSample.sampleUserTable.get(new Get(Bytes.toBytes("person17")));
        assertEquals("content in user table rollbacked correctly",
            "camarao",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
        assertEquals("timestamp of user content is not the start TS of the aborted txn",
            txn5.getStartTimestamp(),
            r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is not the start TS of the aborted txn",
            txn5.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the previous committed txn's end timestamp",
            txn5.getEndTimestamp(),
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r2 = testSample.client.getTimestampData(txn6.getStartTimestamp());
        assertEquals("state of the transaction is \'aborted\'",
            Bytes.toString(Schema.STATE_ABORTED),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        txn7.commit();
    }

    /**
     * Testing whether we don't recover a transaction that has no end
     * timestamp, while reading for the Snapshot Isolated Read Timestamp in user
     * tables.
     *
     * @throws IOException
     */
//    @Test
    public void testDontRecoverTxnWithoutEndTimestamp() throws IOException {
        HAcidTxn txnA = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person17"));
            pput1.add("info","name","monteiro");
        txnA.put(pput1);

        HAcidTxn txnB = new HAcidTxn(testSample.client);
            HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person17"));
            pget1.addColumn("info", "name");
            pget1.addColumn(Schema.FAMILY_HACID, Schema.QUALIFIER_USERTABLE_COMMITTED_AT);
        Result resultGet = txnB.get(pget1);
        // In this get, while searching for the Snapshot Isolated Read Timestamp,
        // txnB should do nothing about txnA

        assertNotSame("content in user table cannot be the active txn's put",
            "monteiro",
            Bytes.toString(resultGet.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
        assertNotSame("timestamp of committed-at cell is not the active txn's start-ts",
            txnA.getStartTimestamp(),
            (
                resultGet.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );

        Result r2 = testSample.client.getTimestampData(txnA.getStartTimestamp());
        assertEquals("state of the transaction is \'active\'",
            Bytes.toString(Schema.STATE_ACTIVE),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        // TODO: HAcidClient method for finding an end ts given a start ts
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            Schema.FAMILY_HACID,
            Schema.QUALIFIER_START_TIMESTAMP,
            CompareFilter.CompareOp.EQUAL,
            Bytes.toBytes(txnA.getStartTimestamp())
        );
        filter.setFilterIfMissing(true);
        Scan scan = new Scan(
            HAcidClient.timestampToKey(txnB.getStartTimestamp()-1L),
            HAcidClient.timestampToKey(txnA.getStartTimestamp())
        );
        scan.setFilter(filter);
        HTable hacid_timestamplog = new HTable(testSample.conf, Schema.TABLE_TIMESTAMP_LOG);
        ResultScanner scanner = hacid_timestamplog.getScanner(scan);
        Result txnA_endTS_res = scanner.next();
        scanner.close();
        assertNull("No end timestamp for the active txn should exist",
            txnA_endTS_res
        );

        txnB.commit();
    }

    @Test
    public void testConcurrentStateDisagreementAndRecovery() throws IOException {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person19"));
            pput1.add("info","name","pekko");
        txn1.put(pput1);
        txn1.commit();

        HAcidTxn txnA = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person19"));
            pput2.add("info","name","johannes");
        txnA.put(pput2);

        txnA.requestEndTimestamp();

        // First set state to abort
        try {
            testSample.client.setStateAborted(txnA);
        } catch (StateDisagreementException ex) {
            LOG.error("Unexpected state disagreement. There should be no concurrent"
                + " deciders in this test.", ex
            );
            return;
        }

        // Then try to set state to commit
        txnA.decideCommitOrAbort();

        Result r1 = testSample.sampleUserTable.get(new Get(Bytes.toBytes("person19")));
        assertEquals("content in user table rollbacked correctly",
            "pekko",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
        assertEquals("timestamp of user content is not the start TS of the aborted txn",
            txn1.getStartTimestamp(),
            r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is not the start TS of the aborted txn",
            txn1.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the previous committed txn's end timestamp",
            txn1.getEndTimestamp(),
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r2 = testSample.client.getTimestampData(txnA.getStartTimestamp());
        assertEquals("state of the transaction is \'aborted\'",
            Bytes.toString(Schema.STATE_ABORTED),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );
    }
}
