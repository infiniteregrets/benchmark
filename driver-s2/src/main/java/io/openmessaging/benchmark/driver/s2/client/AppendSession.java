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
package io.openmessaging.benchmark.driver.s2.client;


import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.v1.AppendAck;
import s2.v1.AppendInput;

/**
 * A pipelined s2s append session for one stream. AppendInput frames are written without waiting for
 * acks; the server acks strictly in FIFO order, one AppendAck per AppendInput. On any failure all
 * in-flight batches fail and the session transparently reopens on the next submit.
 */
public class AppendSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AppendSession.class);

    private final H2Transport transport;
    private final Endpoints endpoints;
    private final String stream;
    private final long maxInflightBytes;
    private final long ackTimeoutMs;
    private final ScheduledExecutorService watchdog;
    private ScheduledFuture<?> watchdogTask;

    private final Object lock = new Object();
    private Stream h2Stream;
    private boolean dead;
    private boolean closed;
    private final Deque<Inflight> inflight = new ArrayDeque<>();
    private long inflightBytes;
    private final Deque<PendingWrite> writeQueue = new ArrayDeque<>();
    private boolean writing;
    private Listener currentListener;

    private static final class Inflight {
        final int recordCount;
        final long meteredBytes;
        final List<CompletableFuture<Void>> futures;
        final long submittedAtNanos;

        Inflight(int recordCount, long meteredBytes, List<CompletableFuture<Void>> futures) {
            this.recordCount = recordCount;
            this.meteredBytes = meteredBytes;
            this.futures = futures;
            this.submittedAtNanos = System.nanoTime();
        }
    }

    private static final class PendingWrite {
        final ByteBuffer frame;

        PendingWrite(ByteBuffer frame) {
            this.frame = frame;
        }
    }

    public AppendSession(
            H2Transport transport,
            Endpoints endpoints,
            String stream,
            long maxInflightBytes,
            long ackTimeoutMs,
            ScheduledExecutorService watchdog) {
        this.transport = transport;
        this.endpoints = endpoints;
        this.stream = stream;
        this.maxInflightBytes = maxInflightBytes;
        this.ackTimeoutMs = ackTimeoutMs;
        this.watchdog = watchdog;
    }

    // Fail the session if the oldest unacked batch has waited longer than the ack timeout, so a
    // stalled-but-open stream recycles instead of hanging its futures.
    private void checkAckDeadline() {
        synchronized (lock) {
            Inflight head = inflight.peek();
            if (head != null
                    && !dead
                    && System.nanoTime() - head.submittedAtNanos
                            > TimeUnit.MILLISECONDS.toNanos(ackTimeoutMs)) {
                onSessionFailureLocked(new IOException("no AppendAck within " + ackTimeoutMs + " ms"));
            }
        }
    }

    // Submit one batch. Blocks the caller while the in-flight window is full. The per-record
    // futures complete when the batch is acked.
    public void submit(AppendInput input, long meteredBytes, List<CompletableFuture<Void>> futures) {
        ByteBuffer frame = Framing.encode(input.toByteArray());
        synchronized (lock) {
            if (closed) {
                fail(futures, new IllegalStateException("append session closed"));
                return;
            }
            try {
                while (!inflight.isEmpty() && inflightBytes + meteredBytes > maxInflightBytes) {
                    lock.wait(TimeUnit.SECONDS.toMillis(30));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(futures, e);
                return;
            }
            if (h2Stream == null || dead) {
                try {
                    open();
                } catch (Exception e) {
                    fail(futures, e);
                    return;
                }
            }
            inflight.add(new Inflight(input.getRecordsCount(), meteredBytes, futures));
            inflightBytes += meteredBytes;
            writeQueue.add(new PendingWrite(frame));
            pumpWritesLocked();
        }
    }

    private void open() throws Exception {
        Session session = transport.appendConnection().get(30, TimeUnit.SECONDS);
        HttpFields.Mutable fields =
                HttpFields.build()
                        .put(HttpHeader.AUTHORIZATION, endpoints.authorization)
                        .put(HttpHeader.CONTENT_TYPE, "s2s/proto")
                        .put(HttpHeader.ACCEPT, "application/protobuf");
        if (endpoints.basinHeader != null) {
            fields.put("s2-basin", endpoints.basinHeader);
        }
        MetaData.Request request =
                new MetaData.Request(
                        "POST",
                        HttpURI.from(endpoints.recordsUri(stream, null).toString()),
                        HttpVersion.HTTP_2,
                        fields);
        Promise.Completable<Stream> promise = new Promise.Completable<>();
        Listener listener = new Listener();
        session.newStream(new HeadersFrame(request, null, false), promise, listener);
        this.h2Stream = promise.get(30, TimeUnit.SECONDS);
        this.currentListener = listener;
        this.dead = false;
        if (watchdogTask == null && ackTimeoutMs > 0) {
            long period = Math.max(ackTimeoutMs / 4, 250);
            watchdogTask =
                    watchdog.scheduleWithFixedDelay(
                            this::checkAckDeadline, period, period, TimeUnit.MILLISECONDS);
        }
    }

    private void pumpWritesLocked() {
        if (writing || dead || h2Stream == null) {
            return;
        }
        PendingWrite next = writeQueue.poll();
        if (next == null) {
            return;
        }
        writing = true;
        Stream target = h2Stream;
        target.data(
                new DataFrame(target.getId(), next.frame, false),
                new Callback() {
                    @Override
                    public void succeeded() {
                        synchronized (lock) {
                            writing = false;
                            pumpWritesLocked();
                        }
                    }

                    @Override
                    public void failed(Throwable x) {
                        onSessionFailure(null, x);
                    }
                });
    }

    private void onAckFrame(Listener source, Framing.Frame frame) {
        if (frame.terminal) {
            int status = frame.statusCode();
            if (status >= 400) {
                onSessionFailure(
                        source,
                        new IOException("append session terminated: HTTP " + status + " " + frame.errorJson()));
            } else {
                onSessionFailure(source, new IOException("append session ended by server"));
            }
            return;
        }
        AppendAck ack;
        try {
            ack = AppendAck.parseFrom(frame.body);
        } catch (InvalidProtocolBufferException e) {
            onSessionFailure(source, e);
            return;
        }
        Inflight acked;
        synchronized (lock) {
            if (source != currentListener) {
                return;
            }
            acked = inflight.poll();
            if (acked == null) {
                onSessionFailureLocked(new IOException("unexpected AppendAck with no batch in flight"));
                return;
            }
            long ackedRecords = ack.getEnd().getSeqNum() - ack.getStart().getSeqNum();
            if (ackedRecords != acked.recordCount) {
                log.warn(
                        "stream {}: acked {} records for a batch of {}",
                        stream,
                        ackedRecords,
                        acked.recordCount);
            }
            inflightBytes -= acked.meteredBytes;
            lock.notifyAll();
        }
        for (CompletableFuture<Void> future : acked.futures) {
            future.complete(null);
        }
    }

    private void onSessionFailure(Listener source, Throwable cause) {
        synchronized (lock) {
            if (source != null && source != currentListener) {
                return;
            }
            onSessionFailureLocked(cause);
        }
    }

    private void onSessionFailureLocked(Throwable cause) {
        if (dead) {
            return;
        }
        dead = true;
        if (!closed) {
            log.warn("append session to {} failed: {}", stream, cause.toString());
        }
        List<Inflight> failed = new ArrayList<>(inflight);
        inflight.clear();
        inflightBytes = 0;
        writeQueue.clear();
        writing = false;
        h2Stream = null;
        lock.notifyAll();
        for (Inflight batch : failed) {
            fail(batch.futures, cause);
        }
    }

    private static void fail(List<CompletableFuture<Void>> futures, Throwable cause) {
        for (CompletableFuture<Void> future : futures) {
            future.completeExceptionally(cause);
        }
    }

    @Override
    public void close() {
        Stream target;
        synchronized (lock) {
            closed = true;
            target = h2Stream;
            if (watchdogTask != null) {
                watchdogTask.cancel(false);
                watchdogTask = null;
            }
        }
        if (target != null) {
            target.data(new DataFrame(target.getId(), ByteBuffer.allocate(0), true), Callback.NOOP);
        }
    }

    private class Listener extends Stream.Listener.Adapter {
        private final Framing.Decoder decoder = new Framing.Decoder();
        private volatile int status = 200;
        private final StringBuilder errorBody = new StringBuilder();

        @Override
        public void onHeaders(Stream stream, HeadersFrame frame) {
            MetaData metaData = frame.getMetaData();
            if (metaData instanceof MetaData.Response) {
                status = ((MetaData.Response) metaData).getStatus();
            }
        }

        @Override
        public void onData(Stream stream, DataFrame frame, Callback callback) {
            try {
                if (status != 200) {
                    errorBody.append(StandardCharsets.UTF_8.decode(frame.getData()));
                    if (frame.isEndStream()) {
                        onSessionFailure(
                                this, new IOException("append session rejected: HTTP " + status + " " + errorBody));
                    }
                } else {
                    for (Framing.Frame decoded : decoder.feed(frame.getData())) {
                        onAckFrame(this, decoded);
                    }
                    if (frame.isEndStream()) {
                        onSessionFailure(this, new IOException("append session closed by server"));
                    }
                }
                callback.succeeded();
            } catch (Throwable t) {
                callback.failed(t);
                onSessionFailure(this, t);
            }
        }

        @Override
        public void onReset(Stream stream, ResetFrame frame) {
            onSessionFailure(this, new IOException("append session reset: " + frame.getError()));
        }

        @Override
        public void onFailure(
                Stream stream, int error, String reason, Throwable failure, Callback callback) {
            onSessionFailure(
                    this, failure != null ? failure : new IOException("stream failure: " + reason));
            callback.succeeded();
        }
    }
}
