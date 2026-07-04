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


import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.http2.api.Session;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.util.Promise;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Owns the Jetty HTTP/2 client. Append sessions each get a dedicated connection: sharing a
 * connection between append streams stalls some of them behind connection-level send flow control
 * (measured as multi-second p99 publish latency). Read sessions share a pool, since the client
 * controls the receive windows and sizes them generously.
 */
public class H2Transport implements AutoCloseable {

    private final Endpoints endpoints;
    private final HTTP2Client http2Client;
    private final SslContextFactory.Client sslContextFactory;

    private final ConnectionPool readPool;

    public H2Transport(Endpoints endpoints, int readConnections) throws Exception {
        this.endpoints = endpoints;
        this.http2Client = new HTTP2Client();
        http2Client.setInitialStreamRecvWindow(8 * 1024 * 1024);
        http2Client.setInitialSessionRecvWindow(64 * 1024 * 1024);
        http2Client.setMaxFrameSize(4 * 1024 * 1024);
        this.sslContextFactory = new SslContextFactory.Client();
        http2Client.addBean(sslContextFactory);
        http2Client.start();
        this.readPool = new ConnectionPool(readConnections);
    }

    // A fresh connection owned by the caller, who must close it via Session.close.
    public CompletableFuture<Session> dedicatedConnection() {
        return connect();
    }

    public CompletableFuture<Session> readConnection() {
        return readPool.next();
    }

    private CompletableFuture<Session> connect() {
        InetSocketAddress address =
                new InetSocketAddress(endpoints.baseUri.getHost(), endpoints.port());
        Promise.Completable<Session> promise = new Promise.Completable<>();
        if (endpoints.isTls()) {
            http2Client.connect(sslContextFactory, address, new Session.Listener.Adapter(), promise);
        } else {
            http2Client.connect(address, new Session.Listener.Adapter(), promise);
        }
        return promise;
    }

    @Override
    public void close() throws Exception {
        http2Client.stop();
    }

    private class ConnectionPool {
        private final CompletableFuture<Session>[] connections;
        private final AtomicInteger next = new AtomicInteger();

        @SuppressWarnings("unchecked")
        ConnectionPool(int size) {
            this.connections = new CompletableFuture[size];
        }

        CompletableFuture<Session> next() {
            int idx = Math.floorMod(next.getAndIncrement(), connections.length);
            synchronized (this) {
                CompletableFuture<Session> conn = connections[idx];
                if (conn == null
                        || conn.isCompletedExceptionally()
                        || (conn.isDone() && conn.join().isClosed())) {
                    conn = connect();
                    connections[idx] = conn;
                }
                return conn;
            }
        }
    }
}
