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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.http2.ErrorCode;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.api.Stream;
import org.eclipse.jetty.http2.frames.DataFrame;
import org.eclipse.jetty.http2.frames.HeadersFrame;
import org.eclipse.jetty.http2.frames.ResetFrame;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.v1.ReadBatch;
import s2.v1.SequencedRecord;

/**
 * An s2s read session for one stream, delivering records to a handler on a dedicated thread. The
 * handler may block: flow control (Jetty data callbacks are only completed after delivery) then
 * pushes back on the server. Reconnects transparently, re-pinning to the next unconsumed sequence
 * number.
 */
public class ReadSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReadSession.class);

    private final H2Transport transport;
    private final Endpoints endpoints;
    private final String stream;
    private final Consumer<SequencedRecord> handler;
    private final ExecutorService executor;

    private volatile boolean closed;
    private volatile Stream currentStream;
    private long nextSeqNum;

    private static final class Chunk {
        final ByteBuffer data;
        final Callback callback;
        final boolean endStream;
        final Throwable failure;

        Chunk(ByteBuffer data, Callback callback, boolean endStream) {
            this.data = data;
            this.callback = callback;
            this.endStream = endStream;
            this.failure = null;
        }

        Chunk(Throwable failure) {
            this.data = null;
            this.callback = null;
            this.endStream = true;
            this.failure = failure;
        }
    }

    public ReadSession(
            H2Transport transport,
            Endpoints endpoints,
            String stream,
            long startSeqNum,
            Consumer<SequencedRecord> handler) {
        this.transport = transport;
        this.endpoints = endpoints;
        this.stream = stream;
        this.nextSeqNum = startSeqNum;
        this.handler = handler;
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread thread = new Thread(r, "s2-read-" + stream);
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.submit(this::runLoop);
    }

    private void runLoop() {
        while (!closed) {
            try {
                runOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (!closed) {
                    log.warn("read session on {} failed, reconnecting: {}", stream, e.toString());
                }
            }
            if (!closed) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void runOnce() throws Exception {
        LinkedBlockingQueue<Chunk> chunks = new LinkedBlockingQueue<>();
        Session session = transport.readConnection().get(30, TimeUnit.SECONDS);
        HttpFields.Mutable fields =
                HttpFields.build()
                        .put(HttpHeader.AUTHORIZATION, endpoints.authorization)
                        .put(HttpHeader.CONTENT_TYPE, "s2s/proto")
                        .put(HttpHeader.ACCEPT, "application/protobuf");
        if (endpoints.basinHeader != null) {
            fields.put("s2-basin", endpoints.basinHeader);
        }
        String query = "seq_num=" + nextSeqNum + "&clamp=true";
        MetaData.Request request =
                new MetaData.Request(
                        "GET",
                        HttpURI.from(endpoints.recordsUri(stream, query).toString()),
                        HttpVersion.HTTP_2,
                        fields);
        Promise.Completable<Stream> promise = new Promise.Completable<>();
        CompletableFuture<Integer> status = new CompletableFuture<>();
        session.newStream(
                new HeadersFrame(request, null, true),
                promise,
                new Stream.Listener.Adapter() {
                    @Override
                    public void onHeaders(Stream stream, HeadersFrame frame) {
                        MetaData metaData = frame.getMetaData();
                        if (metaData instanceof MetaData.Response) {
                            status.complete(((MetaData.Response) metaData).getStatus());
                        }
                        if (frame.isEndStream()) {
                            chunks.add(new Chunk(ByteBuffer.allocate(0), Callback.NOOP, true));
                        }
                    }

                    @Override
                    public void onData(Stream stream, DataFrame frame, Callback callback) {
                        chunks.add(new Chunk(frame.getData(), callback, frame.isEndStream()));
                    }

                    @Override
                    public void onReset(Stream stream, ResetFrame frame) {
                        chunks.add(new Chunk(new IOException("stream reset: " + frame.getError())));
                    }

                    @Override
                    public void onFailure(
                            Stream stream, int error, String reason, Throwable failure, Callback callback) {
                        chunks.add(
                                new Chunk(
                                        failure != null ? failure : new IOException("stream failure: " + reason)));
                        callback.succeeded();
                    }
                });
        currentStream = promise.get(30, TimeUnit.SECONDS);
        try {
            int httpStatus = status.get(30, TimeUnit.SECONDS);
            if (httpStatus != 200) {
                String body = drainErrorBody(chunks);
                throw new IOException("read session rejected: HTTP " + httpStatus + " " + body);
            }
            Framing.Decoder decoder = new Framing.Decoder();
            while (!closed) {
                Chunk chunk = chunks.take();
                if (chunk.failure != null) {
                    throw new IOException(chunk.failure);
                }
                try {
                    for (Framing.Frame frame : decoder.feed(chunk.data)) {
                        if (frame.terminal) {
                            int code = frame.statusCode();
                            if (code >= 400) {
                                throw new IOException(
                                        "read session terminated: HTTP " + code + " " + frame.errorJson());
                            }
                            return;
                        }
                        ReadBatch batch = ReadBatch.parseFrom(frame.body);
                        for (SequencedRecord record : batch.getRecordsList()) {
                            handler.accept(record);
                            nextSeqNum = record.getSeqNum() + 1;
                        }
                    }
                } finally {
                    chunk.callback.succeeded();
                }
                if (chunk.endStream) {
                    throw new IOException("read session ended without terminal frame");
                }
            }
        } finally {
            resetCurrentStream();
        }
    }

    private String drainErrorBody(LinkedBlockingQueue<Chunk> chunks) throws InterruptedException {
        StringBuilder body = new StringBuilder();
        while (true) {
            Chunk chunk = chunks.poll(10, TimeUnit.SECONDS);
            if (chunk == null || chunk.failure != null) {
                break;
            }
            body.append(StandardCharsets.UTF_8.decode(chunk.data));
            chunk.callback.succeeded();
            if (chunk.endStream) {
                break;
            }
        }
        return body.toString();
    }

    private void resetCurrentStream() {
        Stream target = currentStream;
        currentStream = null;
        if (target != null && !target.isReset()) {
            target.reset(
                    new ResetFrame(target.getId(), ErrorCode.CANCEL_STREAM_ERROR.code), Callback.NOOP);
        }
    }

    @Override
    public void close() {
        closed = true;
        resetCurrentStream();
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
