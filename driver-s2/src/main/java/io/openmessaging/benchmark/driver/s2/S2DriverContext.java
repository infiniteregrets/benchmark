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


import io.openmessaging.benchmark.driver.s2.client.Endpoints;
import io.openmessaging.benchmark.driver.s2.client.H2Transport;
import io.openmessaging.benchmark.driver.s2.client.ManagementClient;
import java.util.function.Consumer;

/** Shared driver state handed to producers and consumers. */
final class S2DriverContext {
    final ManagementClient management;
    final H2Transport transport;
    final Endpoints endpoints;
    final S2Config config;
    final Consumer<String> streamCreated;

    S2DriverContext(
            ManagementClient management,
            H2Transport transport,
            Endpoints endpoints,
            S2Config config,
            Consumer<String> streamCreated) {
        this.management = management;
        this.transport = transport;
        this.endpoints = endpoints;
        this.config = config;
        this.streamCreated = streamCreated;
    }
}
