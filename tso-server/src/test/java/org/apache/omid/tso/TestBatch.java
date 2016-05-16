/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import org.apache.commons.pool2.PooledObject;
import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatch.class);

    private static final int BATCH_SIZE = 1000;

    private static final long ANY_ST = 1231;
    private static final long ANY_CT = 2241;

    @Mock
    private Channel channel;
    @Mock
    private MonitoringContext monCtx;

    @Test(timeOut = 10_000)
    public void testBatchFunctionality() {

        // Component to test
        Batch batch = new Batch(0, BATCH_SIZE);

        // Test initial state is OK
        assertTrue(batch.isEmpty(), "Batch should be empty");
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

        // Test getting or setting an element in the batch greater than the current number of events is illegal
        try {
            batch.get(1);
            fail();
        } catch (IllegalStateException ex) {
            // Expected, as we can not access elements in the batch greater than the current number of events
        }
        try {
            batch.set(1, new PersistEvent());
            fail();
        } catch (IllegalStateException ex) {
            // Expected, as we can not access elements in the batch greater than the current number of events
        }

        // Test when filling the batch with different types of events, that becomes full
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i % 4 == 0) {
                batch.addTimestamp(ANY_ST, channel, monCtx);
            } else if (i % 4 == 1) {
                batch.addCommit(ANY_ST, ANY_CT, channel, monCtx);
            } else if (i % 4 == 2) {
                batch.addCommitRetry(ANY_ST, channel, monCtx);
            } else {
                batch.addAbort(ANY_ST, channel, monCtx);
            }
        }
        assertFalse(batch.isEmpty(), "Batch should contain elements");
        assertTrue(batch.isFull(), "Batch should be full");
        assertEquals(batch.getNumEvents(), BATCH_SIZE, "Num events should be " + BATCH_SIZE);

        // Test an exception is thrown when batch is full and a new element is going to be added
        try {
            batch.addCommit(ANY_ST, ANY_CT, channel, monCtx);
            fail("Should throw an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "batch is full", "message returned doesn't match");
            LOG.debug("IllegalStateException catch properly");
        }
        assertTrue(batch.isFull(), "Batch shouldn't be empty");

        // Check the first 3 events and the last one correspond to the filling done above
        assertTrue(batch.get(0).getType().equals(PersistEvent.Type.TIMESTAMP));
        assertTrue(batch.get(1).getType().equals(PersistEvent.Type.COMMIT));
        assertTrue(batch.get(2).getType().equals(PersistEvent.Type.COMMIT_RETRY));
        assertTrue(batch.get(3).getType().equals(PersistEvent.Type.ABORT));

        // Set a new value for last element in Batch and check we obtain the right result
        batch.decreaseNumEvents();
        assertEquals(batch.getNumEvents(), BATCH_SIZE - 1, "Num events should be " + (BATCH_SIZE - 1));
        try {
            batch.get(BATCH_SIZE - 1);
            fail();
        } catch (IllegalStateException ex) {
            // Expected, as we can not access elements in the batch greater than the current number of events
        }

        // Re-check that batch is NOT full
        assertFalse(batch.isFull(), "Batch shouldn't be full");

        // Clear the batch and goes back to its initial state
        batch.clear();
        assertTrue(batch.isEmpty(), "Batch should be empty");
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

    }

    @Test(timeOut = 10_000)
    public void testBatchFactoryFunctionality() throws Exception {

        // Component to test
        Batch.BatchFactory factory = new Batch.BatchFactory(BATCH_SIZE);

        // Check the factory creates a new batch properly...
        Batch batch = factory.create();
        assertTrue(batch.isEmpty(), "Batch should be empty");
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

        // ...and is wrapped in to a pooled object
        PooledObject<Batch> pooledBatch = factory.wrap(batch);
        assertEquals(pooledBatch.getObject(), batch);

        // Put some elements in the batch...
        batch.addTimestamp(ANY_ST, channel, monCtx);
        batch.addCommit(ANY_ST, ANY_CT, channel, monCtx);
        batch.addCommitRetry(ANY_ST, channel, monCtx);
        batch.addAbort(ANY_ST, channel, monCtx);
        assertFalse(batch.isEmpty(), "Batch should contain elements");
        assertFalse(batch.isFull(), "Batch should NOT be full");
        assertEquals(batch.getNumEvents(), 4, "Num events should be 4");

        // ... and passivate the object through the factory. It should reset the state of the batch
        factory.passivateObject(pooledBatch);
        assertTrue(batch.isEmpty(), "Batch should NOT contain elements");
        assertFalse(batch.isFull(), "Batch should NOT be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

    }

}