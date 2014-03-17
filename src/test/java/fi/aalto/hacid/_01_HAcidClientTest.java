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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests HAcidClient methods.
 * Beware, these tests should be performed on a clean (empty) HBase site.
 *
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
public class _01_HAcidClientTest {

    private static HAcidTestSample1 testSample;

    public _01_HAcidClientTest() {

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
    public void setUp() throws Exception {  
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of close method, of class HAcidClient.
     */
    //@Test
    public void testClose() throws Exception {
        // T O D O
    }

    /**
     * Test of getNewTimestamp method, of class HAcidClient.
     */
    @Test
    public void testGetNewTimestamp() throws Exception {

        long firstTS  = testSample.client.requestStartTimestamp();
        long secondTS = testSample.client.requestStartTimestamp();
        long thirdTS  = testSample.client.requestStartTimestamp();
        long forthTS  = testSample.client.requestStartTimestamp();
        long fifthTS  = testSample.client.requestStartTimestamp();

        assertEquals("timestamp progression", firstTS+1L, secondTS);
        assertEquals("timestamp progression", secondTS+1L, thirdTS);
        assertEquals("timestamp progression", thirdTS+1L, forthTS);
        assertEquals("timestamp progression", forthTS+1L, fifthTS);
    }

    /**
     * Test of insertInCommitQueue method, of class HAcidClient.
     */
    //@Test
    public void testInsertInCommitQueue() throws Exception {

    }

    /**
     * Test of canCommit method, of class HAcidClient.
     */
    //@Test
    public void testCanCommit() throws Exception {

    }

    /**
     * Test of commit method, of class HAcidClient.
     */
    //@Test
    public void testCommit() throws Exception {

    }

    /**
     * Test of abort method, of class HAcidClient.
     */
    //@Test
    public void testAbort() throws Exception {

    }

    /**
     * Test of isInstalled method, of class HAcidClient.
     */
    //@Test
    public void testIsInstalled() throws Exception {
        
    }

    /**
     * Test of prepareUserTable method, of class HAcidClient.
     */
    //@Test
    public void testPrepareUserTable() throws Exception {

    }
}