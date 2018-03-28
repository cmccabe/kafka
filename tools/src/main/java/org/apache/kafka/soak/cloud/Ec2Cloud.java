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

package org.apache.kafka.soak.cloud;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.soak.cluster.SoakNode;
import org.apache.kafka.soak.common.SoakConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public final class Ec2Cloud implements Cloud, Runnable {
    /**
     * The minimum delay to observe between queuing a request and performing it.
     * Having a coalsce delay helps increase the amount of batching we do.
     * By waiting for a short period, we may encounter other requests that could
     * be done in the same batch.
     */
    private final static int COALSCE_DELAY_MS = 20;

    /**
     * The minimum delay to observe between making a request and making a
     * subsequent request.  This helps avoid exceeding the ec2 rate limiting.
     */
    private final static int CALL_DELAY_MS = 500;

    private static final String IMAGE_ID_DEFAULT = "ami-29ebb519";

    private static final String INSTANCE_TYPE_DEFAULT = "m1.small";

    private final String keyPair;
    private final String securityGroup;
    private final AmazonEC2 ec2;
    private final Thread thread;
    private final List<RunInstance> runs = new ArrayList<>();
    private final List<DescribeInstance> describes = new ArrayList<>();
    private final List<TerminateInstance> terminates = new ArrayList<>();
    private boolean shouldExit = false;
    private long nextCallTimeMs = 0;

    private static final class RunInstance {
        private final KafkaFutureImpl<String> future;
        private final TreeMap<String, String> params;

        RunInstance(KafkaFutureImpl<String> future, TreeMap<String, String> params) {
            this.future = future;
            this.params = params;
        }
    }

    private static final class DescribeInstance {
        private final KafkaFutureImpl<TreeMap<String, String>> future;
        private final String instanceId;

        DescribeInstance(KafkaFutureImpl<TreeMap<String, String>> future, String instanceId) {
            this.future = future;
            this.instanceId = instanceId;
        }
    }

    private static final class TerminateInstance {
        private final KafkaFutureImpl<Void> future;
        private final String instanceId;

        TerminateInstance(KafkaFutureImpl<Void> future, String instanceId) {
            this.future = future;
            this.instanceId = instanceId;
        }
    }

    public Ec2Cloud() {
        this("", "");
    }

    public Ec2Cloud(String keyPair, String securityGroup) {
        this.keyPair = keyPair;
        this.securityGroup = securityGroup;
        this.ec2 = AmazonEC2ClientBuilder.defaultClient();
        this.thread = new Thread(this, "Ec2CloudThread");
        this.thread.start();
    }

    @Override
    public void run() {
        try {
            while (true) {
                synchronized (this) {
                    long delay = calculateDelayMs();
                    if (delay < 0) {
                        break;
                    } else if (delay > 0) {
                        wait(delay);
                    } else {
                        makeCalls();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            synchronized (this) {
                RuntimeException e = new RuntimeException("Ec2Cloud is shutting down.");
                for (RunInstance run : runs) {
                    run.future.completeExceptionally(e);
                }
                for (DescribeInstance describe : describes) {
                    describe.future.completeExceptionally(e);
                }
                for (TerminateInstance terminate : terminates) {
                    terminate.future.completeExceptionally(e);
                }
            }
        }
    }

    private synchronized long calculateDelayMs() throws Exception {
        if (shouldExit) {
            // Should exit.
            return -1;
        } else if (runs.isEmpty() && describes.isEmpty() && terminates.isEmpty()) {
            // Nothing to do.
            return Long.MAX_VALUE;
        } else {
            long now = Time.SYSTEM.milliseconds();
            if (nextCallTimeMs > now) {
                return nextCallTimeMs - now;
            } else {
                return 0;
            }
        }
    }

    private String getWithDefault(String key, Map<String, String> map, String keyDefault) {
        String value = map.get(key);
        if (value != null) {
            return value;
        }
        return keyDefault;
    }

    private synchronized void makeCalls() throws Exception {
        if (!runs.isEmpty()) {
            List<RunInstance> batchRuns = new ArrayList<>();
            Iterator<RunInstance> iter = runs.iterator();
            RunInstance firstRun = iter.next();
            iter.remove();
            Map<String, String> firstParams = firstRun.params;
            batchRuns.add(firstRun);
            while (iter.hasNext()) {
                RunInstance runInstance = iter.next();
                if (runInstance.params.equals(firstParams)) {
                    batchRuns.add(runInstance);
                    iter.remove();
                }
            }
            RunInstancesRequest req = new RunInstancesRequest()
                .withImageId(getWithDefault(SoakConfig.IMAGE_ID, firstRun.params, IMAGE_ID_DEFAULT))
                .withInstanceType(getWithDefault(SoakConfig.INSTANCE_TYPE, firstRun.params,
                    INSTANCE_TYPE_DEFAULT))
                .withMinCount(batchRuns.size())
                .withMaxCount(batchRuns.size())
                .withKeyName(keyPair)
                .withSecurityGroups(securityGroup);
            RunInstancesResult result = ec2.runInstances(req);
            Reservation reservation = result.getReservation();
            Iterator<RunInstance> runInstanceIterator = batchRuns.iterator();
            Iterator<Instance> instanceIterator = reservation.getInstances().iterator();
            while (runInstanceIterator.hasNext() && instanceIterator.hasNext()) {
                RunInstance runInstance = runInstanceIterator.next();
                Instance instance = instanceIterator.next();
                runInstance.future.complete(instance.getInstanceId());
            }
            while (runInstanceIterator.hasNext()) {
                RunInstance runInstance = runInstanceIterator.next();
                runInstance.future.completeExceptionally(
                    new RuntimeException("Unable to create instance"));
            }
        } else if (!describes.isEmpty()) {
            Map<String, DescribeInstance> idToDescribe = new HashMap<>();
            for (Iterator<DescribeInstance> iter = describes.iterator(); iter.hasNext();
                     iter.remove()) {
                DescribeInstance describe = iter.next();
                idToDescribe.put(describe.instanceId, describe);
            }
            DescribeInstancesRequest req = new DescribeInstancesRequest()
                .withInstanceIds(idToDescribe.keySet());
            DescribeInstancesResult result = ec2.describeInstances(req);
            for (Reservation reservation : result.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    DescribeInstance describeInstance = idToDescribe.get(instance.getInstanceId());
                    if (describeInstance != null) {
                        TreeMap<String, String> status = new TreeMap<>();
                        status.put(SoakConfig.PRIVATE_DNS, instance.getPrivateDnsName());
                        status.put(SoakConfig.PUBLIC_DNS, instance.getPublicDnsName());
                        describeInstance.future.complete(status);
                        idToDescribe.remove(instance.getInstanceId());
                    }
                }
                for (Map.Entry<String, DescribeInstance> entry : idToDescribe.entrySet()) {
                    entry.getValue().future.completeExceptionally(
                        new RuntimeException("Result did not include instance id " + entry.getKey()));
                }
            }
        } else if (!terminates.isEmpty()) {
            Map<String, TerminateInstance> idToTerminate = new HashMap<>();
            for (Iterator<TerminateInstance> iter = terminates.iterator(); iter.hasNext();
                     iter.remove()) {
                TerminateInstance terminate = iter.next();
                idToTerminate.put(terminate.instanceId, terminate);
            }
            TerminateInstancesRequest req = new TerminateInstancesRequest()
                .withInstanceIds(idToTerminate.keySet());
            ec2.terminateInstances(req);
            for (TerminateInstance terminateInstance : idToTerminate.values()) {
                terminateInstance.future.complete(null);
            }
        }
        updateNextCallTime(CALL_DELAY_MS);
    }

    private synchronized void updateNextCallTime(long minDelay) {
        nextCallTimeMs = Math.max(nextCallTimeMs, Time.SYSTEM.milliseconds() + minDelay);
    }

    @Override
    public void close() throws InterruptedException {
        synchronized (this) {
            shouldExit = true;
            notifyAll();
        }
        thread.join();
        ec2.shutdown();
    }

    @Override
    public String runInstance(Map<String, String> params) throws Exception {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        synchronized (this) {
            runs.add(new RunInstance(future, new TreeMap<>(params)));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future.get();
    }

    @Override
    public Map<String, String> describeInstance(String instanceId) throws Exception {
        KafkaFutureImpl<TreeMap<String, String>> future = new KafkaFutureImpl<>();
        synchronized (this) {
            describes.add(new DescribeInstance(future, instanceId));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future.get();
    }

    private KafkaFutureImpl<Void> terminateInstance(String instanceId) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        synchronized (this) {
            terminates.add(new TerminateInstance(future, instanceId));
            updateNextCallTime(COALSCE_DELAY_MS);
            notifyAll();
        }
        return future;
    }

    @Override
    public void terminateInstances(String... instanceIds) throws Throwable {
        List<KafkaFutureImpl<Void>> futures = new ArrayList<>();
        for (String instanceId : instanceIds) {
            futures.add(terminateInstance(instanceId));
        }
        Throwable exception = null;
        for (KafkaFutureImpl<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (exception == null) {
                    exception = e.getCause();
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public RemoteCommand remoteCommand(SoakNode node) {
        return new SoakRemoteCommand(node);
    }
}
