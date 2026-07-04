/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.s2;


import com.google.protobuf.ByteString;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.s2.client.Endpoints;
import io.openmessaging.benchmark.driver.s2.client.H2Transport;
import io.openmessaging.benchmark.driver.s2.client.ReadSession;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.v1.AppendInput;
import s2.v1.AppendRecord;
import s2.v1.SequencedRecord;

/**
 * Consumer that shares a subscription with peers via S2 itself. S2 has no server-side consumer
 * groups, and OMB scatters the consumers of one subscription across workers, so membership is
 * agreed through a claim stream per (topic, subscription): each member appends a claim record, and
 * the total order of claims gives every member the same view of the group. Member k of n reads the
 * partition streams where partition % n == k, rebalancing as late claims arrive (only during setup,
 * before load starts). Partition streams are created fresh for each run, so reads always start at
 * sequence number 0.
 */
public final class S2BenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(S2BenchmarkConsumer.class);

    private final H2Transport transport;
    private final Endpoints endpoints;
    private final List<String> partitions;
    private final ConsumerCallback callback;
    private final String claimId;

    private final Object lock = new Object();
    private final List<String> claims = new ArrayList<>();
    private final Map<Integer, ReadSession> partitionReaders = new HashMap<>();
    private ReadSession claimReader;
    private boolean closed;

    private S2BenchmarkConsumer(
            H2Transport transport,
            Endpoints endpoints,
            List<String> partitions,
            ConsumerCallback callback) {
        this.transport = transport;
        this.endpoints = endpoints;
        this.partitions = partitions;
        this.callback = callback;
        this.claimId = UUID.randomUUID().toString();
    }

    // Create a consumer: ensure the claim stream, append our claim, then tail the claim stream
    // and (re)assign partitions as members appear. The returned future completes once our own
    // claim is observed, i.e. once this consumer holds a partition assignment.
    public static CompletableFuture<BenchmarkConsumer> create(
            S2DriverContext context,
            String topic,
            String subscriptionName,
            List<String> partitions,
            ConsumerCallback callback) {
        String claimStream = topic + "/g/" + subscriptionName;
        S2BenchmarkConsumer consumer =
                new S2BenchmarkConsumer(context.transport, context.endpoints, partitions, callback);
        CompletableFuture<BenchmarkConsumer> ready = new CompletableFuture<>();
        context
                .management
                .ensureStream(claimStream, context.config.storageClass, context.config.retentionAgeSecs)
                .thenCompose(
                        ignore -> {
                            context.streamCreated.accept(claimStream);
                            ByteString claimBody = ByteString.copyFrom(consumer.claimId, StandardCharsets.UTF_8);
                            AppendInput claim =
                                    AppendInput.newBuilder()
                                            .addRecords(AppendRecord.newBuilder().setBody(claimBody))
                                            .build();
                            return context.management.appendOnce(claimStream, claim);
                        })
                .whenComplete(
                        (ack, error) -> {
                            if (error != null) {
                                ready.completeExceptionally(error);
                                return;
                            }
                            consumer.startClaimReader(claimStream, ready);
                        });
        return ready;
    }

    private void startClaimReader(String claimStream, CompletableFuture<BenchmarkConsumer> ready) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            claimReader =
                    new ReadSession(transport, endpoints, claimStream, 0, record -> onClaim(record, ready));
        }
    }

    private void onClaim(SequencedRecord record, CompletableFuture<BenchmarkConsumer> ready) {
        String member = record.getBody().toStringUtf8();
        synchronized (lock) {
            if (closed || claims.contains(member)) {
                return;
            }
            claims.add(member);
            int self = claims.indexOf(claimId);
            if (self < 0) {
                return;
            }
            rebalanceLocked(self, claims.size());
        }
        if (member.equals(claimId)) {
            ready.complete(this);
        }
    }

    private void rebalanceLocked(int memberIndex, int memberCount) {
        List<Integer> desired = new ArrayList<>();
        for (int p = 0; p < partitions.size(); p++) {
            if (p % memberCount == memberIndex) {
                desired.add(p);
            }
        }
        log.info(
                "consumer {} is member {}/{}, taking partitions {}",
                claimId,
                memberIndex,
                memberCount,
                desired);
        List<Integer> toClose = new ArrayList<>();
        for (Integer held : partitionReaders.keySet()) {
            if (!desired.contains(held)) {
                toClose.add(held);
            }
        }
        for (Integer partition : toClose) {
            partitionReaders.remove(partition).close();
        }
        for (Integer partition : desired) {
            if (!partitionReaders.containsKey(partition)) {
                partitionReaders.put(
                        partition,
                        new ReadSession(transport, endpoints, partitions.get(partition), 0, this::deliver));
            }
        }
    }

    private void deliver(SequencedRecord record) {
        callback.messageReceived(record.getBody().asReadOnlyByteBuffer(), record.getTimestamp());
    }

    @Override
    public void close() throws Exception {
        List<ReadSession> readers = new ArrayList<>();
        synchronized (lock) {
            closed = true;
            if (claimReader != null) {
                readers.add(claimReader);
                claimReader = null;
            }
            readers.addAll(partitionReaders.values());
            partitionReaders.clear();
        }
        for (ReadSession reader : readers) {
            reader.close();
        }
    }
}
