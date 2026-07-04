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


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.s2.client.Endpoints;
import io.openmessaging.benchmark.driver.s2.client.H2Transport;
import io.openmessaging.benchmark.driver.s2.client.ManagementClient;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMB driver for S2 (s2.dev). An OMB topic with N partitions maps to N S2 streams named
 * {topic}/p/{partition}; subscriptions coordinate through a claim stream at
 * {topic}/g/{subscription}.
 */
public class S2BenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(S2BenchmarkDriver.class);

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private S2Config config;
    private Endpoints endpoints;
    private H2Transport transport;
    private ManagementClient management;
    private S2DriverContext context;
    private final Set<String> createdStreams = ConcurrentHashMap.newKeySet();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.config = mapper.readValue(configurationFile, S2Config.class);
        String token = config.token;
        if (token == null || token.isEmpty()) {
            token = System.getenv(config.tokenEnv);
        }
        if (token == null || token.isEmpty()) {
            throw new IOException(
                    "no S2 access token: set `token` in the driver yaml or the "
                            + config.tokenEnv
                            + " environment variable");
        }
        if (config.basin == null || config.basin.isEmpty()) {
            throw new IOException("`basin` must be set in the driver yaml");
        }
        this.endpoints = new Endpoints(config.basinEndpoint, config.basin, token);
        try {
            this.transport =
                    new H2Transport(endpoints, config.resolveConnections(config.http2ReadConnections));
        } catch (Exception e) {
            throw new IOException("failed to start HTTP/2 transport", e);
        }
        this.management = new ManagementClient(endpoints);
        this.context =
                new S2DriverContext(management, transport, endpoints, config, createdStreams::add);
        log.info("S2 driver initialized: basin={} endpoint={}", config.basin, endpoints.baseUri);
    }

    @Override
    public String getTopicNamePrefix() {
        return "s2";
    }

    private static String partitionStream(String topic, int partition) {
        return String.format("%s/p/%05d", topic, partition);
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            String stream = partitionStream(topic, p);
            createdStreams.add(stream);
            futures.add(management.createStream(stream, config.storageClass, config.retentionAgeSecs));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<List<String>> topicPartitions(String topic) {
        return management
                .listStreams(topic + "/p/")
                .thenApply(
                        streams -> {
                            if (streams.isEmpty()) {
                                throw new IllegalStateException("no partition streams for topic " + topic);
                            }
                            Collections.sort(streams);
                            return streams;
                        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return topicPartitions(topic)
                .thenApply(partitions -> new S2BenchmarkProducer(transport, endpoints, config, partitions));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return topicPartitions(topic)
                .thenCompose(
                        partitions ->
                                S2BenchmarkConsumer.create(
                                        context, topic, subscriptionName, partitions, consumerCallback));
    }

    @Override
    public void close() throws Exception {
        if (config != null && config.deleteStreamsOnClose && management != null) {
            List<CompletableFuture<Void>> deletes = new ArrayList<>();
            for (String stream : createdStreams) {
                deletes.add(
                        management
                                .deleteStream(stream)
                                .exceptionally(
                                        e -> {
                                            log.warn("failed to delete stream {}: {}", stream, e.toString());
                                            return null;
                                        }));
            }
            CompletableFuture.allOf(deletes.toArray(new CompletableFuture[0])).join();
        }
        if (transport != null) {
            transport.close();
        }
    }
}
