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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import s2.v1.AppendAck;
import s2.v1.AppendInput;

/** Unary HTTP client for S2 stream management and one-shot appends. */
public class ManagementClient {

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Endpoints endpoints;
    private final HttpClient httpClient;

    public ManagementClient(Endpoints endpoints) {
        this.endpoints = endpoints;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    private HttpRequest.Builder request(URI uri) {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(30))
                        .header("Authorization", endpoints.authorization);
        if (endpoints.basinHeader != null) {
            builder.header("s2-basin", endpoints.basinHeader);
        }
        return builder;
    }

    private ObjectNode streamConfig(String storageClass, long retentionAgeSecs) {
        ObjectNode config = mapper.createObjectNode();
        config.put("storage_class", storageClass);
        config.putObject("retention_policy").put("age", retentionAgeSecs);
        config.putObject("timestamping").put("mode", "client-prefer");
        return config;
    }

    // Create a stream, expecting it to not already exist.
    public CompletableFuture<Void> createStream(
            String stream, String storageClass, long retentionAgeSecs) {
        ObjectNode body = mapper.createObjectNode();
        body.put("stream", stream);
        body.set("config", streamConfig(storageClass, retentionAgeSecs));
        HttpRequest req =
                request(endpoints.baseUri.resolve(endpoints.baseUri.getPath() + "/streams"))
                        .header("Content-Type", "application/json")
                        .header("s2-request-token", UUID.randomUUID().toString())
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                        .build();
        return httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenAccept(rsp -> expect(rsp, 200, 201));
    }

    // Idempotently ensure a stream exists.
    public CompletableFuture<Void> ensureStream(
            String stream, String storageClass, long retentionAgeSecs) {
        HttpRequest req =
                request(endpoints.streamUri(stream))
                        .header("Content-Type", "application/json")
                        .PUT(
                                HttpRequest.BodyPublishers.ofString(
                                        streamConfig(storageClass, retentionAgeSecs).toString()))
                        .build();
        return httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenAccept(rsp -> expect(rsp, 200, 201));
    }

    public CompletableFuture<Void> deleteStream(String stream) {
        HttpRequest req = request(endpoints.streamUri(stream)).DELETE().build();
        return httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenAccept(rsp -> expect(rsp, 200, 202, 204, 404));
    }

    // List all stream names with the given prefix, following pagination.
    public CompletableFuture<List<String>> listStreams(String prefix) {
        List<String> result = new ArrayList<>();
        return listPage(prefix, null, result);
    }

    private CompletableFuture<List<String>> listPage(
            String prefix, String startAfter, List<String> acc) {
        String query = "prefix=" + URLEncoder.encode(prefix, StandardCharsets.UTF_8);
        if (startAfter != null) {
            query += "&start_after=" + URLEncoder.encode(startAfter, StandardCharsets.UTF_8);
        }
        URI uri = endpoints.baseUri.resolve(endpoints.baseUri.getPath() + "/streams?" + query);
        HttpRequest req = request(uri).GET().build();
        return httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenCompose(
                        rsp -> {
                            expect(rsp, 200);
                            try {
                                JsonNode root = mapper.readTree(rsp.body());
                                String last = null;
                                for (JsonNode stream : root.path("streams")) {
                                    String name = stream.path("name").asText();
                                    if (stream.path("deleted_at").isMissingNode()
                                            || stream.path("deleted_at").isNull()) {
                                        acc.add(name);
                                    }
                                    last = name;
                                }
                                if (root.path("has_more").asBoolean(false) && last != null) {
                                    return listPage(prefix, last, acc);
                                }
                                return CompletableFuture.completedFuture(acc);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
    }

    // One-shot unary append, used for coordination records (not the benchmark hot path).
    public CompletableFuture<AppendAck> appendOnce(String stream, AppendInput input) {
        HttpRequest req =
                request(endpoints.recordsUri(stream, null))
                        .header("Content-Type", "application/protobuf")
                        .header("Accept", "application/protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(input.toByteArray()))
                        .build();
        return httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(
                        rsp -> {
                            if (rsp.statusCode() != 200) {
                                throw new IllegalStateException(
                                        "append to "
                                                + stream
                                                + " failed: HTTP "
                                                + rsp.statusCode()
                                                + " "
                                                + new String(rsp.body(), StandardCharsets.UTF_8));
                            }
                            try {
                                return AppendAck.parseFrom(rsp.body());
                            } catch (InvalidProtocolBufferException e) {
                                throw new IllegalStateException("malformed AppendAck", e);
                            }
                        });
    }

    private static void expect(HttpResponse<String> rsp, int... statuses) {
        for (int status : statuses) {
            if (rsp.statusCode() == status) {
                return;
            }
        }
        throw new IllegalStateException(
                "unexpected HTTP "
                        + rsp.statusCode()
                        + " from "
                        + rsp.request().method()
                        + " "
                        + rsp.request().uri()
                        + ": "
                        + rsp.body());
    }
}
