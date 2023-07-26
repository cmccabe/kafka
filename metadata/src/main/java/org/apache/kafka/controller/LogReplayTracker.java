/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;


/**
 * The LogReplayTracker manages state associated with replaying the metadata log, such as whether
 * we have seen any records. It is accessed solely from the quorum controller thread.
 */
class LogReplayTracker {
    static class Builder {
        private LogContext logContext = null;
        private Time time = Time.SYSTEM;
        private int nodeId = -1;
        private QuorumControllerMetrics metrics = null;
        private Consumer<Integer> gainLeadershipCallback = __ -> { };
        private Consumer<Integer> loseLeadershipCallback = __ -> { };
        private FaultHandler fatalFaultHandler = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setMetrics(QuorumControllerMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        Builder setGainLeadershipCallback(Consumer<Integer> gainLeadershipCallback) {
            this.gainLeadershipCallback = gainLeadershipCallback;
            return this;
        }

        Builder setLoseLeadershipCallback(Consumer<Integer> loseLeadershipCallback) {
            this.loseLeadershipCallback = loseLeadershipCallback;
            return this;
        }

        Builder setFatalFaultHandler(FaultHandler fatalFaultHandler) {
            this.fatalFaultHandler = fatalFaultHandler;
            return this;
        }

        LogReplayTracker build() {
            if (logContext == null) logContext = new LogContext();
            if (metrics == null) {
                metrics = new QuorumControllerMetrics(Optional.empty(),
                    time,
                    false);
            }
            if (fatalFaultHandler == null) {
                throw new IllegalStateException("You must specify a fatal fault handler.");
            }
            return new LogReplayTracker(logContext,
                time,
                nodeId,
                metrics,
                gainLeadershipCallback,
                loseLeadershipCallback,
                fatalFaultHandler);
        }
    }

    static String leaderName(OptionalInt leaderId) {
        return leaderId.isPresent() ?
                String.valueOf(leaderId.getAsInt()) :
                "(none)";
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * The ID of this controller.
     */
    private final int nodeId;

    /**
     * The controller metrics.
     */
    private final QuorumControllerMetrics controllerMetrics;

    /**
     * Invoked whenever we gain leadership.
     */
    private final Consumer<Integer> gainLeadershipCallback;

    /**
     * Invoked whenever we lose leadership.
     */
    private final Consumer<Integer> loseLeadershipCallback;

    /**
     * The fault handler to invoke if there is a fatal fault.
     */
    private final FaultHandler fatalFaultHandler;

    /**
     * True if we haven't replayed any records yet.
     */
    private boolean empty;

    /**
     * If the controller is active, the current leader epoch; -1 otherwise.
     * Accessible from multiple threads; written only by the controller event handler thread.
     */
    private volatile int curClaimEpoch;

    /**
     * True if the controller is active; false otherwise.
     */
    private boolean active;

    /**
     * The current leader and epoch of the Raft quorum. Note that if the controller voluntarily
     * resigns, there will be a brief period afterwards during which it will be the leader according
     * to Raft, but not according to itself.
     */
    private LeaderAndEpoch raftLeader;

    /**
     * If the controller is active, the next log offset to write to; 0 otherwise.
     */
    private long nextWriteOffset;

    /**
     * The last offset we have committed, or -1 if we have not committed any offsets.
     */
    private long lastCommittedOffset;

    /**
     * The epoch of the last offset we have committed, or -1 if we have not committed any offsets.
     */
    private int lastCommittedEpoch;

    /**
     * The timestamp in milliseconds of the last batch we have committed, or -1 if we have not committed any offset.
     */
    private long lastCommittedTimestamp;

    private LogReplayTracker(
        LogContext logContext,
        Time time,
        int nodeId,
        QuorumControllerMetrics controllerMetrics,
        Consumer<Integer> gainLeadershipCallback,
        Consumer<Integer> loseLeadershipCallback,
        FaultHandler fatalFaultHandler
    ) {
        this.log = logContext.logger(LogReplayTracker.class);
        this.time = time;
        this.nodeId = nodeId;
        this.controllerMetrics = controllerMetrics;
        this.gainLeadershipCallback = gainLeadershipCallback;
        this.loseLeadershipCallback = loseLeadershipCallback;
        this.fatalFaultHandler = fatalFaultHandler;
        this.empty = true;
        this.curClaimEpoch = -1;
        this.active = false;
        this.raftLeader = LeaderAndEpoch.UNKNOWN;
        this.nextWriteOffset = 0L;
        this.lastCommittedOffset = -1L;
        this.lastCommittedEpoch = -1;
        this.lastCommittedTimestamp = -1L;
        controllerMetrics.setLastCommittedRecordOffset(lastCommittedOffset);
        controllerMetrics.setLastAppliedRecordOffset(lastCommittedOffset);
        controllerMetrics.setLastAppliedRecordTimestamp(lastCommittedTimestamp);
    }

    int nodeId() {
        return nodeId;
    }

    boolean empty() {
        return empty;
    }

    int curClaimEpoch() {
        return curClaimEpoch;
    }

    boolean active() {
        return active;
    }

    void replay(ApiMessage message) {
        empty = false;
    }

    void updateLastCommittedState(
        long offset,
        int epoch,
        long timestamp
    ) {
        lastCommittedOffset = offset;
        lastCommittedEpoch = epoch;
        lastCommittedTimestamp = timestamp;
        controllerMetrics.setLastCommittedRecordOffset(lastCommittedOffset);
        if (!active) {
            controllerMetrics.setLastAppliedRecordOffset(lastCommittedOffset);
            controllerMetrics.setLastAppliedRecordTimestamp(lastCommittedTimestamp);
        }
    }

    long lastCommittedOffset() {
        return lastCommittedOffset;
    }

    int lastCommittedEpoch() {
        return lastCommittedEpoch;
    }

    long lastCommittedTimestamp() {
        return lastCommittedTimestamp;
    }

    void updateNextWriteOffset(long newNextWriteOffset) {
        if (!active) {
            throw new RuntimeException("Cannot update the next write offset when we are not active.");
        }
        this.nextWriteOffset = newNextWriteOffset;
        controllerMetrics.setLastAppliedRecordOffset(newNextWriteOffset - 1);
        // This is not truly the append timestamp. The KRaft client doesn't expose the append
        // time when scheduling a write. Using the current time is good enough, because this
        // is called right after the records were given to the KRaft client for appending, and
        // the default append linger for KRaft is 25ms.
        controllerMetrics.setLastAppliedRecordTimestamp(time.milliseconds());
    }

    /**
     * Called by QuorumController when the controller wants to voluntarily resign.
     */
    void resign() {
        if (!active) {
            if (raftLeader.isLeader(nodeId)) {
                throw fatalFaultHandler.handleFault("Cannot resign at epoch " + raftLeader.epoch() +
                        ", because we already resigned.");
            } else {
                throw fatalFaultHandler.handleFault("Cannot resign at epoch " + raftLeader.epoch() +
                        ", because we are not the Raft leader at this epoch.");
            }
        }
        log.warn("Resigning as the active controller at epoch {}. Reverting to last " +
                "committed offset {}.", curClaimEpoch, lastCommittedOffset);
        becomeInactive(raftLeader.epoch());
    }

    void handleLeaderChange(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        if (newRaftLeader.leaderId().isPresent()) {
            controllerMetrics.incrementNewActiveControllers();
        }
        if (active) {
            if (newRaftLeader.isLeader(nodeId)) {
                handleLeaderChangeActiveToActive(newRaftLeader, newNextWriteOffset);
            } else {
                handleLeaderChangeActiveToStandby(newRaftLeader);
            }
        } else {
            if (newRaftLeader.isLeader(nodeId)) {
                handleLeaderChangeStandbyToActive(newRaftLeader, newNextWriteOffset);
            } else {
                handleLeaderChangeStandbyToStandby(newRaftLeader);
            }
        }
    }

    private void handleLeaderChangeActiveToActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        log.warn("We were the leader in epoch {}, and are still the leader in the new epoch {}.",
                raftLeader.epoch(), newRaftLeader.epoch());
        becomeActive(newRaftLeader, newNextWriteOffset);
    }

    private void handleLeaderChangeActiveToStandby(
        LeaderAndEpoch newRaftLeader
    ) {
        log.warn("Renouncing the leadership due to a metadata log event. " +
            "We were the leader at epoch {}, but in the new epoch {}, " +
            "the leader is {}. Reverting to last committed offset {}.",
            raftLeader.epoch(),
            newRaftLeader.epoch(), 
            leaderName(newRaftLeader.leaderId()),
            lastCommittedOffset);
        this.raftLeader = newRaftLeader;
        becomeInactive(raftLeader.epoch());
    }

    private void handleLeaderChangeStandbyToActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        log.info("Becoming the active controller at epoch {}, new next write offset {}.",
            newRaftLeader.epoch(), newNextWriteOffset);
        becomeActive(newRaftLeader, newNextWriteOffset);
    }

    private void handleLeaderChangeStandbyToStandby(
        LeaderAndEpoch newRaftLeader
    ) {
        log.info("In the new epoch {}, the leader is {}.",
                newRaftLeader.epoch(), leaderName(newRaftLeader.leaderId()));
        this.raftLeader = newRaftLeader;
    }

    private void becomeActive(
        LeaderAndEpoch newRaftLeader,
        long newNextWriteOffset
    ) {
        this.active = true;
        this.raftLeader = newRaftLeader;
        this.curClaimEpoch = newRaftLeader.epoch();
        updateNextWriteOffset(newNextWriteOffset);
        controllerMetrics.setActive(true);
        try {
            gainLeadershipCallback.accept(newRaftLeader.epoch());
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception while claiming leadership", e);
        }
    }

    private void becomeInactive(int epoch) {
        this.active = false;
        this.nextWriteOffset = 0L;
        this.curClaimEpoch = -1;
        controllerMetrics.setActive(false);
        try {
            loseLeadershipCallback.accept(epoch);
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception in loseLeadership callback", e);
        }
    }

    /**
     * Find the latest offset that is safe to perform read operations at.
     *
     * @return The offset.
     */
    long currentReadOffset() {
        if (active) {
            // The active controller keeps an in-memory snapshot at the last committed offset,
            // which we want to read from when performing read operations. This will avoid
            // reading uncommitted data.
            return lastCommittedOffset;
        } else {
            // Standby controllers never have uncommitted data in memory. Therefore, we always
            // read the latest from every data structure.
            return SnapshotRegistry.LATEST_EPOCH;
        }
    }

    LeaderAndEpoch raftLeader() {
        return raftLeader;
    }

    long nextWriteOffset() {
        if (!active) {
            throw new RuntimeException("Cannot access the next write offset when we are not active.");
        }
        return nextWriteOffset;
    }

    QuorumControllerMetrics controllerMetrics() {
        return controllerMetrics;
    }
}
