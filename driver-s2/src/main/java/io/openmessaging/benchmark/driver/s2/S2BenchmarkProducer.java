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
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.s2.client.AppendSession;
import io.openmessaging.benchmark.driver.s2.client.Endpoints;
import io.openmessaging.benchmark.driver.s2.client.H2Transport;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import s2.v1.AppendInput;
import s2.v1.AppendRecord;

/**
 * Producer over one topic. Each message is routed to a partition stream (round-robin, or hashed
 * when a key is present) and accumulated into an AppendInput batch that is flushed on linger expiry
 * or when batch limits are reached. Records carry the send time as their S2 timestamp, which
 * consumers read back for end-to-end latency.
 */
public class S2BenchmarkProducer implements BenchmarkProducer {

    /** Metered bytes for a record with no headers: 8 fixed bytes plus the body length. */
    private static final int RECORD_METERED_OVERHEAD = 8;

    private final S2Config config;
    private final AppendSession[] sessions;
    private final Batch[] batches;
    private final ScheduledExecutorService flusher;
    private final AtomicInteger roundRobin = new AtomicInteger();

    private static final class Batch {
        final List<AppendRecord> records = new ArrayList<>();
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        long meteredBytes;
        boolean flushScheduled;

        void reset() {
            records.clear();
            futures.clear();
            meteredBytes = 0;
            flushScheduled = false;
        }
    }

    public S2BenchmarkProducer(
            H2Transport transport, Endpoints endpoints, S2Config config, List<String> partitions) {
        this.config = config;
        this.sessions = new AppendSession[partitions.size()];
        this.batches = new Batch[partitions.size()];
        this.flusher =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "s2-producer-flush");
                            thread.setDaemon(true);
                            return thread;
                        });
        for (int i = 0; i < partitions.size(); i++) {
            sessions[i] =
                    new AppendSession(
                            transport,
                            endpoints,
                            partitions.get(i),
                            config.maxInflightBytesPerStream,
                            config.appendAckTimeoutMs,
                            flusher);
            batches[i] = new Batch();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        int partition =
                key.map(k -> (k.hashCode() & 0x7fffffff) % sessions.length)
                        .orElseGet(() -> Math.floorMod(roundRobin.getAndIncrement(), sessions.length));
        AppendRecord record =
                AppendRecord.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setBody(ByteString.copyFrom(payload))
                        .build();
        long metered = RECORD_METERED_OVERHEAD + payload.length;
        CompletableFuture<Void> future = new CompletableFuture<>();
        Batch batch = batches[partition];
        synchronized (batch) {
            if (!batch.records.isEmpty()
                    && (batch.meteredBytes + metered > config.maxBatchMeteredBytes
                            || batch.records.size() >= config.maxBatchRecords)) {
                flushLocked(partition, batch);
            }
            batch.records.add(record);
            batch.futures.add(future);
            batch.meteredBytes += metered;
            if (batch.meteredBytes >= config.maxBatchMeteredBytes
                    || batch.records.size() >= config.maxBatchRecords) {
                flushLocked(partition, batch);
            } else if (!batch.flushScheduled) {
                batch.flushScheduled = true;
                flusher.schedule(() -> flush(partition), config.lingerMs, TimeUnit.MILLISECONDS);
            }
        }
        return future;
    }

    private void flush(int partition) {
        Batch batch = batches[partition];
        synchronized (batch) {
            flushLocked(partition, batch);
        }
    }

    private void flushLocked(int partition, Batch batch) {
        if (batch.records.isEmpty()) {
            batch.flushScheduled = false;
            return;
        }
        AppendInput input = AppendInput.newBuilder().addAllRecords(batch.records).build();
        List<CompletableFuture<Void>> futures = new ArrayList<>(batch.futures);
        long metered = batch.meteredBytes;
        batch.reset();
        sessions[partition].submit(input, metered, futures);
    }

    @Override
    public void close() throws Exception {
        for (int i = 0; i < sessions.length; i++) {
            flush(i);
        }
        flusher.shutdown();
        for (AppendSession session : sessions) {
            session.close();
        }
    }
}
