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

/** Configuration mapped from the driver yaml file. */
public class S2Config {

    /**
     * Basin endpoint. A {@code {basin}} placeholder is substituted with the basin name (subdomain
     * addressing). Without a placeholder, the basin is conveyed via the {@code s2-basin} header
     * instead, which is what a local S2 endpoint expects.
     */
    public String basinEndpoint = "https://{basin}.b.aws.s2.dev";

    /** Basin that streams are created in. It must already exist. */
    public String basin;

    /** Access token. When unset, read from the environment variable named by tokenEnv. */
    public String token;

    public String tokenEnv = "S2_ACCESS_TOKEN";

    /** Storage class for created streams: standard or express. */
    public String storageClass = "express";

    /** Retention for created streams, in seconds. */
    public long retentionAgeSecs = 3600;

    // Producer batching.
    public long lingerMs = 1;
    public int maxBatchRecords = 1000;
    public int maxBatchMeteredBytes = 1024 * 1024;
    public long maxInflightBytesPerStream = 5 * 1024 * 1024;

    // HTTP/2 connection pools. Append and read traffic never share a connection.
    public int http2AppendConnections = 2;
    public int http2ReadConnections = 2;

    public boolean deleteStreamsOnClose = true;
}
