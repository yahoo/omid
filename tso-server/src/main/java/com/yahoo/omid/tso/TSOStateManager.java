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
package com.yahoo.omid.tso;

import java.io.IOException;

/**
 * Allows to reset the TSO state and register observers for being notified
 * when changes occur
 */
public interface TSOStateManager {

    /**
     * Represents the state of the TSO
     */
    public static class TSOState {

        // TSO state variables
        private final long lowWatermark;

        public TSOState(long lowWatermark, long epoch) {
            this.lowWatermark = lowWatermark;
        }

        public long getLowWatermark() {
            return lowWatermark;
        }

        public long getEpoch() {
            // In this implementation the epoch == low watermark
            return lowWatermark;
        }

        @Override
        public String toString() {
            return String.format("LWM %d/Epoch %d", getLowWatermark(), getEpoch());
        }

    }

    /**
     * Allows implementors to receive the new state when changes occur
     */
    public interface StateObserver {

        /**
         * Notifies the observer about the change in state
         * @param state
         *            the new TSOState
         * @throws InterruptedException
         */
        public void update(TSOState state) throws IOException, InterruptedException;

    }

    /**
     * Allows to register observers for receiving state changes
     *
     * @param observer
     *            the observer to register
     */
    public void register(StateObserver observer);

    /**
     * Allows to de-register observers for stopping receiving changes
     *
     * @param observer
     *            the observer to unregister
     */
    public void unregister(StateObserver observer);

    /**
     * Allows to reset the state
     *
     * @return the new state after reset
     * @throws IOException
     *             when problems resetting occur
     * @throws InterruptedException
     */
    public TSOState reset() throws IOException, InterruptedException;

}
