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
public class _02_HAcidTxnTest {

    static Logger LOG = Logger.getLogger(_02_HAcidTxnTest.class.getName());
    private static HAcidTestSample1 testSample;

    public _02_HAcidTxnTest() {
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
    }

    /**
     * Test of get method, of class HAcidTxn.
     */
    @Test
    public void testGet() throws Exception {
        HAcidTxn txn = new HAcidTxn(testSample.client);
            HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person1"));
            pget1.addColumn("info", "name");
        Result r1 = txn.get(pget1);

        // Checks ----------------------------------------------
        assertEquals("Content in user table was read correctly in a transaction that didn't write anything.",
            "hotloo",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );

        HAcidPut put1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person1"));
        put1.add("info", "name", "hao tele");
        txn.put(put1);

        Result r2 = txn.get(pget1);

        // Checks ----------------------------------------------
        assertEquals("Transaction does not read what itself wrote.",
            "hotloo",
            Bytes.toString(r2.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );

        HAcidPut put2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person1"));
        put2.add("info", "name", "xiranood");
        txn.put(put2);
        txn.commit();

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidGet pget2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person1"));
            pget2.addColumn("info", "name");
        Result r3 = txn2.get(pget2);
        // Checks ----------------------------------------------
        assertEquals("Next transaction reads what the previous wrote.",
            "xiranood",
            Bytes.toString(r3.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
    }

    /**
     * Test of put method, of class HAcidTxn.
     */
    @Test
    public void testPut() throws Exception {

        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pput1.add("info","name","elyas");
        txn1.put(pput1);

        HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person247"));
        pget1.addColumn("info", "name");

        Result r1 = txn1.get(pget1);
        // Checks ----------------------------------------------
        assertEquals("Cannot read from new row, because it hasnt been committed yet.",
            0,
            r1.size()
        );

        txn1.commit();

        Get g1 = new Get(Bytes.toBytes("person247"));
        g1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        Result r2 = testSample.sampleUserTable.get(g1);
        // Checks ----------------------------------------------
        assertEquals("first new content in user table was correctly put and persisted after commit",
            "elyas",
            Bytes.toString(r2.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pput2.add("info","name", "ferreira");
        txn2.put(pput2);

        HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person247"));
        pput3.add("info","name", "de medeiros");
        txn2.put(pput3);

        txn2.commit();
        Result r5 = testSample.sampleUserTable.get(g1);
        // Checks ----------------------------------------------
        assertEquals("Only the second write gets committed.",
            "de medeiros",
            Bytes.toString(r5.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getValue())
        );
    }

    /**
     * Test of delete method, of class HAcidTxn.
     */
    @Test
    public void testDelete() throws Exception {
        HAcidTxn txnPre = new HAcidTxn(testSample.client);
            HAcidPut pputPre = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pputPre.add("info","country","greenland");
        txnPre.put(pputPre);
        txnPre.commit();

        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pput1.add("info","country","brazil");

            HAcidDelete pdel1 = new HAcidDelete(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pdel1.specifyColumn("info", "country");
        txn1.put(pput1);
        txn1.delete(pdel1);

        HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person247"));
        pget1.addColumn("info", "country");

        Result r1 = txn1.get(pget1);
        // Checks ----------------------------------------------
        assertEquals("Delete has still no effect before committing.",
            "greenland",
            Bytes.toString(r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("country")).getValue())
        );
        
        txn1.commit();

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidGet pget2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person247"));
            pget2.addColumn("info", "country");
        Result r2 = txn2.get(pget2);
        // Checks ----------------------------------------------
        assertEquals("new committed content in user table was correctly deleted after commit.",
            Bytes.toString(Schema.CELL_VALUE_EMPTY),
            Bytes.toString(r2.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("country")).getValue())
        );

        HAcidTxn txn3 = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person31071984"));
            pput2.add("info", "name", "livia");

            HAcidDelete pdel2 = new HAcidDelete((testSample.sampleUserTable), Bytes.toBytes("person31071984"));
        txn3.put(pput2);
        txn3.delete(pdel2);
        txn3.commit();

        HAcidGet pget3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person31071984"));
        pget3.addColumn("info", "name");
        Result r3 = txn2.get(pget2);
        // Checks ----------------------------------------------
        assertNull("new row in user table was correctly strictly deleted after commit.",
            r3.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name"))
        );

        txn2.commit();
        
    }

    /**
     * Test of commit method, of class HAcidTxn.
     */
    @Test
    public void testCommit() throws Exception {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person3"));
            pput1.add("info","wife","jadel");

            HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("person3"));
            pget1.addColumn("info", "name");
        txn1.get(pget1);
        txn1.put(pput1);
        txn1.commit();

        Result r0 = testSample.client.getTimestampData(txn1.getStartTimestamp());
        assertEquals("state of the transaction is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r0.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        Result r1 = testSample.client.getTimestampData(txn1.getEndTimestamp());
        assertEquals("the \'writeset\' column is properly marked",
            Bytes.toString(Schema.MARKER_TRUE),
            Bytes.toString(
                r1.getValue(
                    Schema.FAMILY_WRITESET,
                    Bytes.toBytes("people:person3")
                )
            )
        );
        
        Result r2 = testSample.client.getTimestampData(txn1.getEndTimestamp());
        assertEquals("the \'readset\' column is properly marked",
            Bytes.toString(Schema.MARKER_TRUE),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_READSET,
                    Bytes.toBytes("people:person3")
                )
            )
        );

        Result r3 = testSample.sampleUserTable.get(new Get(Bytes.toBytes("person3")));
        assertEquals("the committed-at cell value is equal to the end timestamp",
            txn1.getEndTimestamp(),
            Bytes.toLong(
                r3.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getValue()
            )
        );
    }

    @Test
    public void testTwoConflictingTransactions() throws Exception {
        HAcidTxn txnA = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person2"));
            pput1.add("info","name","cesar");
        txnA.put(pput1);

        HAcidTxn txnB = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person2"));
            pput2.add("info", "name", "medeiros");
        txnB.put(pput2);

        txnA.commit();
        txnB.commit();

        // check
        Get g1 = new Get(Bytes.toBytes("person2"));
        Result r1 = testSample.sampleUserTable.get(g1);
        assertEquals("content in user table modified correctly",
            "cesar",
            Bytes.toString(r1.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        );
        assertEquals("timestamp of user content is the start TS A",
            txnA.getStartTimestamp(),
            r1.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("name")).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is the start TS A",
            txnA.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the end TS A",
            txnA.getEndTimestamp(),
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r2 = testSample.client.getTimestampData(txnA.getStartTimestamp());
        assertEquals("state of the transaction A is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r2.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );
        Result r3 = testSample.client.getTimestampData(txnB.getStartTimestamp());
        assertEquals("state of the transaction B is \'aborted\'",
            Bytes.toString(Schema.STATE_ABORTED),
            Bytes.toString(
                r3.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );
    }

    @Test
    public void testFirstUpdatedDoesNotStartCommitted() throws Exception {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(
                testSample.sampleUserTable,
                Bytes.toBytes("person19876")
            );
            pput1.add("info","name","silvio santos");
        txn1.put(pput1);

        // check that committed-at is the null timestamp
        Get g1 = new Get(Bytes.toBytes("person19876"));
        Result r1 = testSample.sampleUserTable.get(g1);
        assertEquals("value of committed-at cell is the null timestamp",
            Schema.TIMESTAMP_NULL_LONG,
            Bytes.toLong(
                r1.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );
        assertEquals("timestamp of committed-at cell is correct",
            txn1.getStartTimestamp(),
            (
                r1.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );

        // check that the uncommitted row inserted is not considered committed
        // through a HAcidGet operation
        HAcidTxn txn2 = new HAcidTxn(testSample.client);
        HAcidGet pget1 = new HAcidGet(
            testSample.sampleUserTable,
            Bytes.toBytes("person19876")
        );
        Result r3 = txn2.get(pget1);
        assertTrue("Result of a HAcidTxn.get for that row must be null",
            r3.isEmpty()
        );
        txn2.commit();

        // finally commit the new row
        txn1.commit();
        Result r4 = testSample.sampleUserTable.get(g1);

        assertEquals("content in user table modified correctly",
            "silvio santos",
            Bytes.toString(
                r4.getColumnLatest(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name")
                )
                .getValue()
            )
        );
        assertEquals("timestamp of user content is the start TS",
            txn1.getStartTimestamp(),
            r4.getColumnLatest(
                Bytes.toBytes("info"),
                Bytes.toBytes("name")
            ).getTimestamp()
        );
        assertEquals("timestamp of committed-at cell is the start TS",
            txn1.getStartTimestamp(),
            (
                r4.getColumnLatest(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
                .getTimestamp()
            )
        );
        assertEquals("value of committed-at cell is the end TS",
            txn1.getEndTimestamp(),
            Bytes.toLong(
                r4.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_USERTABLE_COMMITTED_AT
                )
            )
        );

        Result r5 = testSample.client.getTimestampData(txn1.getStartTimestamp());
        assertEquals("state of the transaction is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r5.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );
    }

    @Test
    public void testReadOnlyTransaction() throws Exception {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(
                testSample.sampleUserTable,
                Bytes.toBytes("person19876")
            );
            pput1.add("info","name","TV man");
        txn1.put(pput1);
        txn1.commit();

        // The read-only transaction
        HAcidTxn txnR = new HAcidTxn(testSample.client);
            HAcidGet pget = new HAcidGet(
                testSample.sampleUserTable,
                Bytes.toBytes("person19876")
            );
            pget.addColumn("info", "name");
        Result readResult = txnR.get(pget);
        boolean commitResult = txnR.commit();

        assertEquals("The value read was correct",
            "TV man",
            Bytes.toString(
                readResult.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))
            )
        );

        assertEquals("Result of commit() is \'committed\'",
            true,
            commitResult
        );

        assertEquals("Read-only txn has end-ts equal to start-ts (in memory)",
            txnR.getStartTimestamp(),
            txnR.getEndTimestamp()
        );

        Result r5 = testSample.client.getTimestampData(txnR.getStartTimestamp());
        assertEquals("Read-only txn has end-ts equal to start-ts (in Timestamp Log)",
            HAcidClient.keyToTimestamp(r5.getRow()),
            Bytes.toLong(r5.getValue(Schema.FAMILY_HACID, Schema.QUALIFIER_END_TIMESTAMP))
        );

        assertEquals("State of the read-only txn is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r5.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );
    }

    @Test
    public void testSnapshotIsolation1() throws Exception {
        // Txn that commits before the others
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(
                testSample.sampleUserTable,
                Bytes.toBytes("person3112")
            );
            pput1.add("info","name","rosileia");
        txn1.put(pput1);
        txn1.commit();

        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable,
                Bytes.toBytes("person3112")
            );
            pput2.add("info","name","fonseca");
            HAcidGet pget2 = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person3112")
            );
            pget2.addColumn("info", "name");
        txn2.put(pput2);
        Result res2 = txn2.get(pget2);

        assertEquals("SI: txn2 reads data previous txn1, not from itself",
            "rosileia",
            Bytes.toString(
                res2.getValue(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name")
                )
            )
        );

        HAcidTxn txn3 = new HAcidTxn(testSample.client);
            HAcidGet pget3 = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person3112")
            );
            pget3.addColumn("info", "name");
        Result res3 = txn3.get(pget3);

        assertEquals("SI: txn3 reads data from txn1, not from txn2",
            "rosileia",
            Bytes.toString(
                res3.getValue(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name")
                )
            )
        );

        txn2.commit();
        txn3.commit();
    }

    @Test
    public void testMarblesSnapshotIsolation() throws IOException {
        testSample.client.setIsolation(HAcidClient.IsolationLevel.SNAPSHOT_ISOLATION);
        
        // INTERLEAVING: A reads, B reads, A writes, B writes, A commits, B commits
        {
            // Making marbles
            HAcidTxn txn1 = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput1.add("info","color","black");

                HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput2.add("info","color","black");

                HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput3.add("info","color","white");

                HAcidPut pput4 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput4.add("info","color","white");
            txn1.put(pput1);
            txn1.put(pput2);
            txn1.put(pput3);
            txn1.put(pput4);
            txn1.commit();

            // Txn A "Black-to-white" READS
            HAcidTxn txnA = new HAcidTxn(testSample.client);
                HAcidGet pgetA1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetA1.addColumn("info", "color");

                HAcidGet pgetA2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetA2.addColumn("info", "color");

                HAcidGet pgetA3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetA3.addColumn("info", "color");

                HAcidGet pgetA4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetA4.addColumn("info", "color");
            byte[] readA1 = txnA.get(pgetA1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA2 = txnA.get(pgetA2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA3 = txnA.get(pgetA3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA4 = txnA.get(pgetA4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readA1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readA2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readA3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readA4)
            );

            // Txn B "White-to-black" READS
            HAcidTxn txnB = new HAcidTxn(testSample.client);
                HAcidGet pgetB1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetB1.addColumn("info", "color");

                HAcidGet pgetB2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetB2.addColumn("info", "color");

                HAcidGet pgetB3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetB3.addColumn("info", "color");

                HAcidGet pgetB4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetB4.addColumn("info", "color");
            byte[] readB1 = txnB.get(pgetB1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB2 = txnB.get(pgetB2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB3 = txnB.get(pgetB3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB4 = txnB.get(pgetB4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readB1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readB2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readB3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readB4)
            );

            // Txn A "Black-to-white" WRITES
            if(Bytes.equals(readA1,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA2,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA3,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA4,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }

            // Txn B "White-to-black" WRITES
            if(Bytes.equals(readB1,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB2,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB3,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB4,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }

            // Commit A and B
            assertTrue("Txn A committed", txnA.commit());
            assertTrue("Txn B committed", txnB.commit());

            // Txn for checking how things went
            HAcidTxn txnZ = new HAcidTxn(testSample.client);
                HAcidGet pgetZ1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetZ1.addColumn("info", "color");

                HAcidGet pgetZ2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetZ2.addColumn("info", "color");

                HAcidGet pgetZ3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetZ3.addColumn("info", "color");

                HAcidGet pgetZ4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetZ4.addColumn("info", "color");
            byte[] readZ1 = txnZ.get(pgetZ1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ2 = txnZ.get(pgetZ2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ3 = txnZ.get(pgetZ3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ4 = txnZ.get(pgetZ4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            txnZ.commit();

            // Asserts come here.
            assertEquals("marble 1 is white",
                "white",
                Bytes.toString(readZ1)
            );
            assertEquals("marble 2 is white",
                "white",
                Bytes.toString(readZ2)
            );
            assertEquals("marble 3 is black",
                "black",
                Bytes.toString(readZ3)
            );
            assertEquals("marble 4 is black",
                "black",
                Bytes.toString(readZ4)
            );
        }

        // INTERLEAVING: A reads, A writes, B reads, B writes, A commits, B commits
        {
            // Making marbles
            HAcidTxn txn1 = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput1.add("info","color","black");

                HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput2.add("info","color","black");

                HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput3.add("info","color","white");

                HAcidPut pput4 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput4.add("info","color","white");
            txn1.put(pput1);
            txn1.put(pput2);
            txn1.put(pput3);
            txn1.put(pput4);
            txn1.commit();

            // Txn A "Black-to-white" READS
            HAcidTxn txnA = new HAcidTxn(testSample.client);
                HAcidGet pgetA1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetA1.addColumn("info", "color");

                HAcidGet pgetA2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetA2.addColumn("info", "color");

                HAcidGet pgetA3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetA3.addColumn("info", "color");

                HAcidGet pgetA4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetA4.addColumn("info", "color");
            byte[] readA1 = txnA.get(pgetA1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA2 = txnA.get(pgetA2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA3 = txnA.get(pgetA3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA4 = txnA.get(pgetA4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readA1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readA2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readA3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readA4)
            );

            // Txn A "Black-to-white" WRITES
            if(Bytes.equals(readA1,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA2,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA3,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA4,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }

            // Txn B "White-to-black" READS
            HAcidTxn txnB = new HAcidTxn(testSample.client);
                HAcidGet pgetB1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetB1.addColumn("info", "color");

                HAcidGet pgetB2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetB2.addColumn("info", "color");

                HAcidGet pgetB3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetB3.addColumn("info", "color");

                HAcidGet pgetB4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetB4.addColumn("info", "color");
            byte[] readB1 = txnB.get(pgetB1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB2 = txnB.get(pgetB2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB3 = txnB.get(pgetB3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB4 = txnB.get(pgetB4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readB1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readB2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readB3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readB4)
            );

            // Txn B "White-to-black" WRITES
            if(Bytes.equals(readB1,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB2,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB3,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB4,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }

            // Commit A and B
            assertTrue("Txn A committed", txnA.commit());
            assertTrue("Txn B committed", txnB.commit());

            // Txn for checking how things went
            HAcidTxn txnZ = new HAcidTxn(testSample.client);
                HAcidGet pgetZ1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetZ1.addColumn("info", "color");

                HAcidGet pgetZ2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetZ2.addColumn("info", "color");

                HAcidGet pgetZ3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetZ3.addColumn("info", "color");

                HAcidGet pgetZ4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetZ4.addColumn("info", "color");
            byte[] readZ1 = txnZ.get(pgetZ1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ2 = txnZ.get(pgetZ2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ3 = txnZ.get(pgetZ3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ4 = txnZ.get(pgetZ4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            txnZ.commit();

            // Asserts come here.
            assertEquals("marble 1 is white",
                "white",
                Bytes.toString(readZ1)
            );
            assertEquals("marble 2 is white",
                "white",
                Bytes.toString(readZ2)
            );
            assertEquals("marble 3 is black",
                "black",
                Bytes.toString(readZ3)
            );
            assertEquals("marble 4 is black",
                "black",
                Bytes.toString(readZ4)
            );
        }
    }

    @Test
    public void testWriteSnapshotIsolation1() throws IOException {
        testSample.client.setIsolation(HAcidClient.IsolationLevel.WRITE_SNAPSHOT_ISOLATION);

        // INTERLEAVING: w1[A=x] w2[A=y] c2 c1 r3[A=x] c3
        {
            HAcidTxn txn1 = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("rowA"));
                pput1.add("info","value","x");
            txn1.put(pput1);

            HAcidTxn txn2 = new HAcidTxn(testSample.client);
                HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("rowA"));
                pput2.add("info","value","y");
            txn2.put(pput2);

            assertEquals("txn2 commits",
                true,
                txn2.commit()
            );

            assertEquals("txn1 commits",
                true,
                txn1.commit()
            );

            HAcidTxn txn3 = new HAcidTxn(testSample.client);
                HAcidGet pget1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("rowA"));
                pget1.addColumn("info", "value");

            byte[] readA = txn3.get(pget1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("value")).getValue();
            txn3.commit();

            assertEquals("The value read is from the transaction that committed latest: txn1.",
                "x",
                Bytes.toString(readA)
            );
        }

        // Switch back to default isolation level
        testSample.client.setIsolation(HAcidClient.IsolationLevel.SNAPSHOT_ISOLATION);
    }

    @Test
    public void testMarblesWriteSnapshotIsolation() throws IOException {
        testSample.client.setIsolation(HAcidClient.IsolationLevel.WRITE_SNAPSHOT_ISOLATION);

        // INTERLEAVING: A reads, B reads, A writes, B writes, A commits, B commits
        {
            // Making marbles
            HAcidTxn txn1 = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput1.add("info","color","black");

                HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput2.add("info","color","black");

                HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput3.add("info","color","white");

                HAcidPut pput4 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput4.add("info","color","white");
            txn1.put(pput1);
            txn1.put(pput2);
            txn1.put(pput3);
            txn1.put(pput4);
            txn1.commit();

            // Txn A "Black-to-white" READS
            HAcidTxn txnA = new HAcidTxn(testSample.client);
                HAcidGet pgetA1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetA1.addColumn("info", "color");

                HAcidGet pgetA2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetA2.addColumn("info", "color");

                HAcidGet pgetA3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetA3.addColumn("info", "color");

                HAcidGet pgetA4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetA4.addColumn("info", "color");
            byte[] readA1 = txnA.get(pgetA1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA2 = txnA.get(pgetA2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA3 = txnA.get(pgetA3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA4 = txnA.get(pgetA4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readA1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readA2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readA3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readA4)
            );

            // Txn B "White-to-black" READS
            HAcidTxn txnB = new HAcidTxn(testSample.client);
                HAcidGet pgetB1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetB1.addColumn("info", "color");

                HAcidGet pgetB2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetB2.addColumn("info", "color");

                HAcidGet pgetB3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetB3.addColumn("info", "color");

                HAcidGet pgetB4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetB4.addColumn("info", "color");
            byte[] readB1 = txnB.get(pgetB1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB2 = txnB.get(pgetB2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB3 = txnB.get(pgetB3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB4 = txnB.get(pgetB4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readB1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readB2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readB3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readB4)
            );

            // Txn A "Black-to-white" WRITES
            if(Bytes.equals(readA1,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA2,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA3,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA4,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }

            // Txn B "White-to-black" WRITES
            if(Bytes.equals(readB1,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB2,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB3,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB4,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }

            // Commit A and B
            assertTrue("Txn A committed", txnA.commit());
            assertTrue("Txn B aborted", !txnB.commit());

            // Txn for checking how things went
            HAcidTxn txnZ = new HAcidTxn(testSample.client);
                HAcidGet pgetZ1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetZ1.addColumn("info", "color");

                HAcidGet pgetZ2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetZ2.addColumn("info", "color");

                HAcidGet pgetZ3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetZ3.addColumn("info", "color");

                HAcidGet pgetZ4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetZ4.addColumn("info", "color");
            byte[] readZ1 = txnZ.get(pgetZ1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ2 = txnZ.get(pgetZ2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ3 = txnZ.get(pgetZ3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ4 = txnZ.get(pgetZ4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            txnZ.commit();

            // Asserts come here.
            assertEquals("marble 1 is white",
                "white",
                Bytes.toString(readZ1)
            );
            assertEquals("marble 2 is white",
                "white",
                Bytes.toString(readZ2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readZ3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readZ4)
            );
        }

        // INTERLEAVING: A reads, A writes, B reads, B writes, B commits, A commits
        {
            // Making marbles
            HAcidTxn txn1 = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput1.add("info","color","black");

                HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput2.add("info","color","black");

                HAcidPut pput3 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput3.add("info","color","white");

                HAcidPut pput4 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput4.add("info","color","white");
            txn1.put(pput1);
            txn1.put(pput2);
            txn1.put(pput3);
            txn1.put(pput4);
            txn1.commit();

            // Txn A "Black-to-white" READS
            HAcidTxn txnA = new HAcidTxn(testSample.client);
                HAcidGet pgetA1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetA1.addColumn("info", "color");

                HAcidGet pgetA2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetA2.addColumn("info", "color");

                HAcidGet pgetA3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetA3.addColumn("info", "color");

                HAcidGet pgetA4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetA4.addColumn("info", "color");
            byte[] readA1 = txnA.get(pgetA1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA2 = txnA.get(pgetA2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA3 = txnA.get(pgetA3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readA4 = txnA.get(pgetA4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readA1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readA2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readA3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readA4)
            );

            // Txn A "Black-to-white" WRITES
            if(Bytes.equals(readA1,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA2,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA3,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }
            if(Bytes.equals(readA4,Bytes.toBytes("black"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "white");
                txnA.put(pput);
            }

            // Txn B "White-to-black" READS
            HAcidTxn txnB = new HAcidTxn(testSample.client);
                HAcidGet pgetB1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetB1.addColumn("info", "color");

                HAcidGet pgetB2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetB2.addColumn("info", "color");

                HAcidGet pgetB3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetB3.addColumn("info", "color");

                HAcidGet pgetB4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetB4.addColumn("info", "color");
            byte[] readB1 = txnB.get(pgetB1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB2 = txnB.get(pgetB2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB3 = txnB.get(pgetB3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readB4 = txnB.get(pgetB4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readB1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readB2)
            );
            assertEquals("marble 3 is white",
                "white",
                Bytes.toString(readB3)
            );
            assertEquals("marble 4 is white",
                "white",
                Bytes.toString(readB4)
            );

            // Txn B "White-to-black" WRITES
            if(Bytes.equals(readB1,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB2,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB3,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }
            if(Bytes.equals(readB4,Bytes.toBytes("white"))){
                HAcidPut pput = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pput.add("info", "color", "black");
                txnB.put(pput);
            }

            // Commit B and A
            assertTrue("Txn B committed", txnB.commit());
            assertTrue("Txn A aborted", !txnA.commit());

            // Txn for checking how things went
            HAcidTxn txnZ = new HAcidTxn(testSample.client);
                HAcidGet pgetZ1 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble1"));
                pgetZ1.addColumn("info", "color");

                HAcidGet pgetZ2 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble2"));
                pgetZ2.addColumn("info", "color");

                HAcidGet pgetZ3 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble3"));
                pgetZ3.addColumn("info", "color");

                HAcidGet pgetZ4 = new HAcidGet(testSample.sampleUserTable, Bytes.toBytes("marble4"));
                pgetZ4.addColumn("info", "color");
            byte[] readZ1 = txnZ.get(pgetZ1).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ2 = txnZ.get(pgetZ2).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ3 = txnZ.get(pgetZ3).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            byte[] readZ4 = txnZ.get(pgetZ4).getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("color")).getValue();
            txnZ.commit();

            // Asserts come here.
            assertEquals("marble 1 is black",
                "black",
                Bytes.toString(readZ1)
            );
            assertEquals("marble 2 is black",
                "black",
                Bytes.toString(readZ2)
            );
            assertEquals("marble 3 is black",
                "black",
                Bytes.toString(readZ3)
            );
            assertEquals("marble 4 is black",
                "black",
                Bytes.toString(readZ4)
            );
        }

        // Switch back to default isolation level
        testSample.client.setIsolation(HAcidClient.IsolationLevel.SNAPSHOT_ISOLATION);
    }

    @Test
    public void testRestoreTxn() throws IOException, Exception {
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person20"));
            pput1.add("info","name","raul");
        txn1.put(pput1);
        txn1.commit();

        assertNull("If you supply a non-end timestamp to HAcidTxn.restore(),"
            + " you must get a null result.",
            HAcidTxn.restore(testSample.client, txn1.getStartTimestamp())
        );

        HAcidTxn restoredTxn1 = HAcidTxn.restore(testSample.client, txn1.getEndTimestamp());

        assertEquals("Restored txn has same start timestamp as original txn",
            txn1.getStartTimestamp(),
            restoredTxn1.getStartTimestamp()
        );
        assertEquals("Restored txn has same end timestamp as original txn",
            txn1.getEndTimestamp(),
            restoredTxn1.getEndTimestamp()
        );

        Collection<byte[]> originalWriteSet = txn1.getWriteset();
        Collection<byte[]> restoredWriteSet = restoredTxn1.getWriteset();
        if(restoredWriteSet.containsAll(originalWriteSet) == false) {
            assertTrue("Restored txn has same writeset entry as original txn", false);
        }

        Collection<byte[]> originalReadSet = txn1.getReadset();
        Collection<byte[]> restoredReadSet = restoredTxn1.getReadset();
        if(restoredReadSet.containsAll(originalReadSet) == false) {
            assertTrue("Restored txn has same readset entry as original txn", false);
        }
    }

    /**
     * The sorting of addColumn() calls in HAcidGet should be irrelevant for
     * the outcome of HAcidTxn.get(HAcidGet)
     * @throws IOException
     */
    @Test
    public void testHAcidGetInsertionOrders() throws IOException {
        // Txn that writes and commits
        HAcidTxn txn1 = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(
                testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pput1.add("info","name","jose da silva");
            pput1.add("info","age","28");
            pput1.add("info","dog","woof");
            pput1.add("info","car","fiat");
        txn1.put(pput1);
        txn1.commit();

        // Txn that reads
        HAcidTxn txn2 = new HAcidTxn(testSample.client);
            HAcidGet pget2a = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2a.addColumn("info", "name");
            pget2a.addColumn("info", "dog");
            pget2a.addColumn("info", "car");
            pget2a.addColumn("info", "age");

            HAcidGet pget2b = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2b.addColumn("info", "age");
            pget2b.addColumn("info", "car");
            pget2b.addColumn("info", "dog");
            pget2b.addColumn("info", "name");

            HAcidGet pget2c = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2c.addColumn("info", "name");
            pget2c.addColumn("info", "age");
            pget2c.addColumn("info", "dog");
            pget2c.addColumn("info", "car");

            HAcidGet pget2d = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2d.addColumn("info", "name");
            pget2d.addColumn("info", "dog");
            pget2d.addColumn("info", "age");
            pget2d.addColumn("info", "car");

            HAcidGet pget2e = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2e.addColumn("info", "age");
            pget2e.addColumn("info", "name");
            pget2e.addColumn("info", "car");
            pget2e.addColumn("info", "dog");

            HAcidGet pget2f = new HAcidGet(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pget2f.addColumn("info", "dog");
            pget2f.addColumn("info", "age");
            pget2f.addColumn("info", "name");
            pget2f.addColumn("info", "car");

            HAcidPut pput2 = new HAcidPut(testSample.sampleUserTable,
                Bytes.toBytes("person147369")
            );
            pput2.add("info","country","brazil");

        LinkedList<Result> results = new LinkedList<Result>();
        Result res2a = txn2.get(pget2a); results.add(res2a);
        Result res2b = txn2.get(pget2b); results.add(res2b);
        Result res2c = txn2.get(pget2c); results.add(res2c);
        Result res2d = txn2.get(pget2d); results.add(res2d);
        Result res2e = txn2.get(pget2e); results.add(res2e);
        Result res2f = txn2.get(pget2f); results.add(res2f);
        txn2.put(pput2);
        txn2.commit();

        // The sorting of addColumn() calls in HAcidGet should be irrelevant for
        // the outcome of HAcidTxn.get(HAcidGet)
        for(Result res : results) {
            assertEquals("HAcidGet returned correct value",
                "28",
                Bytes.toString(
                    res.getValue(
                        Bytes.toBytes("info"),
                        Bytes.toBytes("age")
                    )
                )
            );
            assertEquals("HAcidGet returned correct value",
                "fiat",
                Bytes.toString(
                    res.getValue(
                        Bytes.toBytes("info"),
                        Bytes.toBytes("car")
                    )
                )
            );
            assertEquals("HAcidGet returned correct value",
                "woof",
                Bytes.toString(
                    res.getValue(
                        Bytes.toBytes("info"),
                        Bytes.toBytes("dog")
                    )
                )
            );
            assertEquals("HAcidGet returned correct value",
                "jose da silva",
                Bytes.toString(
                    res.getValue(
                        Bytes.toBytes("info"),
                        Bytes.toBytes("name")
                    )
                )
            );
        }
    }
}
