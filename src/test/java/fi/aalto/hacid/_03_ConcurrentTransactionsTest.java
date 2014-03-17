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
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Correctness tests under concurrent assumptions
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class _03_ConcurrentTransactionsTest {

    static Logger LOG = Logger.getLogger(_03_ConcurrentTransactionsTest.class.getName());
    private static HAcidTestSample1 testSample;

    public _03_ConcurrentTransactionsTest() {
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

    private class ConcurrentCommitPart1 extends Thread {
        HAcidTxn _txn;
        public ConcurrentCommitPart1(HAcidTxn txn) {
            super();
            _txn = txn;
        }
        @Override public void run() {
            try {
                _txn.requestEndTimestamp();
            } catch (Exception ex) {
                Logger.getLogger(_03_ConcurrentTransactionsTest.class.getName()).error(ex);
            }
        }
    }

    private class ConcurrentCommitPart2 extends Thread {
        HAcidTxn _txn;
        public ConcurrentCommitPart2(HAcidTxn txn) {
            super();
            _txn = txn;
        }
        @Override public void run() {
            try {
                _txn.decideCommitOrAbort();
            } catch (Exception ex) {
                Logger.getLogger(_03_ConcurrentTransactionsTest.class.getName()).error(ex);
            }
        }
    }

    private class ConcurrentExecuteTxnOperations extends Thread {
        HAcidTxn _txn;
        List<HAcidPut> operations;
        public ConcurrentExecuteTxnOperations(HAcidTxn txn, List<HAcidPut> op) {
            super();
            _txn = txn;
            operations = op;
        }
        @Override public void run() {
            try {
                for (HAcidPut p : operations) {
                    _txn.put(p);
                }
            } catch(Exception ex) {
                Logger.getLogger(_03_ConcurrentTransactionsTest.class.getName()).error(ex);
            }
        }
    }

    /**
     * Testing canCommit() in the presence of a previous active transaction.
     * The Timestamp Log should see commit/abort decisions happen in FIFO.
     * If transaction 'A' has commit timestamp prior to the commit timestamp of
     * transaction 'B', then 'A' must be decided before 'B'.
     *
     * <p> In this test, two transactions are tested, but they have no write
     * conflicts.
     *
     * @throws Exception
     */
    @Test
    public void testPreviousActiveTxnNoConflicts() throws Exception {
        HAcidTxn txnA = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person1"));
            pput1.add("info","name","hotloo");
        txnA.put(pput1);

        // Necessary because HTable is not thread-safe
        HAcidClient client2 = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTable2 = new HAcidTable(testSample.conf, "people");

        HAcidTxn txnB = new HAcidTxn(client2);
            HAcidPut pput2 = new HAcidPut(sampleUserTable2, Bytes.toBytes("person2"));
            pput2.add("info", "name", "andre");
        txnB.put(pput2);

        LOG.trace("Testing canCommit() in the presence of a previous active transaction that does not conflict");

        LOG.trace("Registering the commit request of transaction A");
        txnA.requestEndTimestamp();
        LOG.trace("Registering the commit request of transaction B");
        txnB.requestEndTimestamp();

        LOG.trace("Performing the commit decision of transaction B");
        ConcurrentCommitPart2 txnBcommit_part2 = new ConcurrentCommitPart2(txnB);
        txnBcommit_part2.start();

        LOG.trace("Performing the commit decision of transaction A");
        ConcurrentCommitPart2 txnAcommit_part2 = new ConcurrentCommitPart2(txnA);
        txnAcommit_part2.start();

        txnBcommit_part2.join(60000);
        txnAcommit_part2.join(60000);

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
        assertEquals("state of the transaction B is \'committed\'",
            Bytes.toString(Schema.STATE_COMMITTED),
            Bytes.toString(
                r3.getValue(
                    Schema.FAMILY_HACID,
                    Schema.QUALIFIER_TXN_STATE
                )
            )
        );

        sampleUserTable2.close();
        sampleUserTable2 = null;
        client2.close();
        client2 = null;
    }

    /**
     * Testing canCommit() in the presence of a previous active transaction.
     * The Timestamp Log should see commit/abort decisions happen in FIFO. If 
     * transaction 'A' has commit timestamp prior to the commit timestamp of
     * transaction 'B', then 'A' must be decided before 'B'.
     *
     * <p> In this test, two conflicting transactions are tested.
     *
     * @throws Exception
     */
    @Test
    public void testPreviousActiveConflictingTxn() throws Exception {
        HAcidTxn txnA = new HAcidTxn(testSample.client);
            HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person2"));
            pput1.add("info","name","cesar");
        txnA.put(pput1);

        // Necessary because HTable is not thread-safe
        HAcidClient client2 = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTable2 = new HAcidTable(testSample.conf, "people");

        HAcidTxn txnB = new HAcidTxn(client2);
            HAcidPut pput2 = new HAcidPut(sampleUserTable2, Bytes.toBytes("person2"));
            pput2.add("info", "name", "andre");
        txnB.put(pput2);

        LOG.trace("Testing canCommit() in the presence of a previous active conflicting transaction");

        LOG.trace("Registering the commit request of transaction A");
        txnA.requestEndTimestamp();
        LOG.trace("Registering the commit request of transaction B");
        txnB.requestEndTimestamp();

        LOG.trace("Performing the commit decision of transaction B");
        ConcurrentCommitPart2 txnBcommit_part2 = new ConcurrentCommitPart2(txnB);
        txnBcommit_part2.start();

        LOG.trace("Performing the commit decision of transaction A");
        ConcurrentCommitPart2 txnAcommit_part2 = new ConcurrentCommitPart2(txnA);
        txnAcommit_part2.start();

        txnBcommit_part2.join(60000);
        txnAcommit_part2.join(60000);

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

        sampleUserTable2.close();
        sampleUserTable2 = null;
        client2.close();
        client2 = null;
    }

    /**
     * Testing canCommit() in the presence of a previous active transaction.
     * The Timestamp Log should see commit/abort decisions happen in FIFO. If
     * transaction 'A' has end timestamp prior to the end timestamp of
     * transaction 'B', then 'A' must be decided before 'B'.
     *
     * <p> In this test, there are three transaction: A, B, C, ordered by end
     * timestamp. A conflicts with B, and B conflicts with C, but A does not
     * conflict with C.
     *
     * <p> What should happen: <br />
     * B tries to get a decision, but waits for A to be decided. <br />
     * A gets decided with 'commit'. <br />
     * C tries to get a decision, but waits for B to be decided. <br />
     * B gets decided with 'abort'. <br />
     * C gets decided with 'commit'. 
     *
     * @throws Exception
     */
    @Test
    public void testMultiplePreviousActiveConflictingTxn() throws Exception {
        
        int retries = 40;

        // Necessary because HTable is not thread-safe
        HAcidClient clientB = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTableB = new HAcidTable(testSample.conf, "people");
        HAcidClient clientC = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTableC = new HAcidTable(testSample.conf, "people");

        // Retries several times to allow different thread interleaving
        // to simulate concurrent execution in a distributed environment
        for(int i=0; i<retries; i++) {
            HAcidTxn txnA = new HAcidTxn(testSample.client);
                HAcidPut pput1 = new HAcidPut(testSample.sampleUserTable, Bytes.toBytes("person1"));
                pput1.add("info","name","hotloo xiranood");
            List<HAcidPut> listA = new LinkedList<HAcidPut>();
            listA.add(pput1);

            HAcidTxn txnB = new HAcidTxn(clientB);
                HAcidPut pput2 = new HAcidPut(sampleUserTableB, Bytes.toBytes("person1"));
                pput2.add("info", "name", "Hotloo");

                HAcidPut pput3 = new HAcidPut(sampleUserTableB, Bytes.toBytes("person2"));
                pput3.add("info", "name", "Andre");

                HAcidPut pput4 = new HAcidPut(sampleUserTableB, Bytes.toBytes("person3"));
                pput4.add("info", "name", "Daniel");
            List<HAcidPut> listB = new LinkedList<HAcidPut>();
            listB.add(pput2);
            listB.add(pput3);
            listB.add(pput4);

            HAcidTxn txnC = new HAcidTxn(clientC);
                HAcidPut pput5 = new HAcidPut(sampleUserTableC, Bytes.toBytes("person3"));
                pput5.add("info", "name", "daniel dos santos");
            List<HAcidPut> listC = new LinkedList<HAcidPut>();
            listC.add(pput5);

            LOG.trace("Testing canCommit() in the presence of a previous "
                + "active conflicting transaction. "
                + "Attempt #"+i);

            ConcurrentExecuteTxnOperations txnA_thread =
                new ConcurrentExecuteTxnOperations(txnA, listA)
            ;
            ConcurrentExecuteTxnOperations txnB_thread =
                new ConcurrentExecuteTxnOperations(txnB, listB)
            ;
            ConcurrentExecuteTxnOperations txnC_thread =
                new ConcurrentExecuteTxnOperations(txnC, listC)
            ;

            txnA_thread.start();
            txnB_thread.start();
            txnC_thread.start();

            txnA_thread.join(60000);
            txnB_thread.join(60000);
            txnC_thread.join(60000);

            ConcurrentCommitPart1 txnA_part1 = new ConcurrentCommitPart1(txnA);
            ConcurrentCommitPart1 txnB_part1 = new ConcurrentCommitPart1(txnB);
            ConcurrentCommitPart1 txnC_part1 = new ConcurrentCommitPart1(txnC);

            LOG.trace("Registering the commit request of transaction A"+txnA.toStringHash());
            txnA_part1.start();
            LOG.trace("Registering the commit request of transaction B"+txnB.toStringHash());
            txnB_part1.start();
            LOG.trace("Registering the commit request of transaction C"+txnC.toStringHash());
            txnC_part1.start();

            txnA_part1.join(60000);
            txnB_part1.join(60000);
            txnC_part1.join(60000);
            
            ConcurrentCommitPart2 txnA_part2 = new ConcurrentCommitPart2(txnA);
            ConcurrentCommitPart2 txnB_part2 = new ConcurrentCommitPart2(txnB);
            ConcurrentCommitPart2 txnC_part2 = new ConcurrentCommitPart2(txnC);

            LOG.trace("Performing the commit decision of transaction A"+txnA.toStringHash());
            txnA_part2.start();
            LOG.trace("Performing the commit decision of transaction B"+txnB.toStringHash());
            txnB_part2.start();
            LOG.trace("Performing the commit decision of transaction C"+txnC.toStringHash());
            txnC_part2.start();

            txnA_part2.join(60000);
            txnB_part2.join(60000);
            txnC_part2.join(60000);

            Result resA = testSample.client.getTimestampData(txnA.getStartTimestamp());
            Result resB = testSample.client.getTimestampData(txnB.getStartTimestamp());
            Result resC = testSample.client.getTimestampData(txnC.getStartTimestamp());

            // CASE A<B<C
            if(
                txnA.getEndTimestamp() < txnB.getEndTimestamp()
                &&
                txnB.getEndTimestamp() < txnC.getEndTimestamp()
            ){
                assertEquals("state of the transaction A is \'committed\'",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resA.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction B is \'aborted\'",
                    Bytes.toString(Schema.STATE_ABORTED),
                    Bytes.toString(
                        resB.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction C should be \'committed\', "
                    +"but \'aborted\' is also an accepted outcome",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resC.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
            }
            // CASE C<B<A
            else if(
                txnC.getEndTimestamp() < txnB.getEndTimestamp()
                &&
                txnB.getEndTimestamp() < txnA.getEndTimestamp()
            ){
                assertEquals("state of the transaction C is \'committed\'",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resC.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction B is \'aborted\'",
                    Bytes.toString(Schema.STATE_ABORTED),
                    Bytes.toString(
                        resB.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction A should be \'committed\', "
                    +"but \'aborted\' is also an accepted outcome",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resA.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
            }
            // CASE B<A<C or B<C<A
            else if(txnB.getEndTimestamp() < txnA.getEndTimestamp()
            && txnB.getEndTimestamp() < txnC.getEndTimestamp())
            {
                assertEquals("state of the transaction B is \'committed\'",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resB.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction A is \'aborted\'",
                    Bytes.toString(Schema.STATE_ABORTED),
                    Bytes.toString(
                        resA.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction C is \'aborted\'",
                    Bytes.toString(Schema.STATE_ABORTED),
                    Bytes.toString(
                        resC.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
            }
            // Case A<C<B or C<A<B
            else if(txnA.getEndTimestamp() < txnB.getEndTimestamp()
            && txnC.getEndTimestamp() < txnB.getEndTimestamp())
            {
                assertEquals("state of the transaction A is \'committed\'",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resA.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction C is \'committed\'",
                    Bytes.toString(Schema.STATE_COMMITTED),
                    Bytes.toString(
                        resC.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
                assertEquals("state of the transaction B is \'aborted\'",
                    Bytes.toString(Schema.STATE_ABORTED),
                    Bytes.toString(
                        resB.getValue(
                            Schema.FAMILY_HACID,
                            Schema.QUALIFIER_TXN_STATE
                        )
                    )
                );
            }
        }
        sampleUserTableB.close();
        sampleUserTableB = null;
        clientB.close();
        clientB = null;
        sampleUserTableC.close();
        sampleUserTableC = null;
        clientC.close();
        clientC = null;
    }

    /**
     * If transaction A has a end timestamp prior to the end timestamp of
     * transaction B, then at the time B is put in the Timestamp Log we must see
     * transaction A in the Timestamp Log. This test checks for this condition.
     */
    @Test
    public void testAtomicSteps3And4() throws Exception {

        int retries = 100;

        // Necessary because HTable is not thread-safe
        HAcidClient clientA = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTableA = new HAcidTable(testSample.conf, "people");
        HAcidClient clientB = new HAcidClient(testSample.conf);
        HAcidTable sampleUserTableB = new HAcidTable(testSample.conf, "people");

        HAcidTxn txnBefore = null;

        // Retries several times to allow different thread interleaving
        // to simulate concurrent execution in a distributed environment
        for(int i=0; i<retries; i++) {
            HAcidTxn txnA = new HAcidTxn(clientA);
                HAcidPut pput1 = new HAcidPut(sampleUserTableA, Bytes.toBytes("person7"));
                pput1.add("info","name","Smith");
            txnA.put(pput1);

            HAcidTxn txnB = new HAcidTxn(clientB);
                HAcidPut pput2 = new HAcidPut(sampleUserTableB, Bytes.toBytes("person8"));
                pput2.add("info", "name", "Johnson");
            txnB.put(pput2);

            ConcurrentCommitPart1 txnAcommit_part1 = new ConcurrentCommitPart1(txnA);
            ConcurrentCommitPart1 txnBcommit_part1 = new ConcurrentCommitPart1(txnB);

            // Commit Part 1
            txnAcommit_part1.start();
            txnBcommit_part1.start();

            // Commit Part 2
            txnBcommit_part1.join(60000);
            if(txnA.getEndTimestamp() == Schema.TIMESTAMP_NULL_LONG
            || (
                txnA.getEndTimestamp() != Schema.TIMESTAMP_NULL_LONG
                &&
                txnA.getEndTimestamp() > txnB.getEndTimestamp()
               )
            ){
                txnBefore = txnB;
            }
            else {
                txnBefore = txnA;
            }

            Result rSTS = testSample.client.getTimestampData(txnBefore.getStartTimestamp());
            Result rCTS = testSample.client.getTimestampData(txnBefore.getEndTimestamp());
            txnB.decideCommitOrAbort();
            // ---
            txnAcommit_part1.join(60000);
            txnA.decideCommitOrAbort();

            assertEquals("state of txn "+txnBefore.toStringHash()+" was \'active\'",
                Bytes.toString(Schema.STATE_ACTIVE),
                Bytes.toString(
                    rSTS.getValue(
                        Schema.FAMILY_HACID,
                        Schema.QUALIFIER_TXN_STATE
                    )
                )
            );
            assertEquals("type of the end timestamp of txn "+txnBefore.toStringHash()+" was \'end\'",
                Bytes.toString(Schema.TYPE_END),
                Bytes.toString(
                    rCTS.getValue(
                        Schema.FAMILY_HACID,
                        Schema.QUALIFIER_TS_TYPE
                    )
                )
            );
        }

        txnBefore = null;

        sampleUserTableA.close();
        sampleUserTableA = null;
        clientA.close();
        clientA = null;
        sampleUserTableB.close();
        sampleUserTableB = null;
        clientB.close();
        clientB = null;
    }

}
