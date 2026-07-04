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


import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Resolves the basin endpoint. A {@code {basin}} placeholder in the configured endpoint selects
 * subdomain addressing; without it, the basin is conveyed via the {@code s2-basin} header, which is
 * what single-host (e.g. local) S2 endpoints expect.
 */
public final class Endpoints {

    /** Base URI for basin-scoped operations, ending in /v1 (no trailing slash). */
    public final URI baseUri;

    /** Value for the s2-basin header, or null when the basin is addressed via the subdomain. */
    public final String basinHeader;

    public final String authorization;

    public Endpoints(String basinEndpoint, String basin, String token) {
        String endpoint = basinEndpoint;
        if (endpoint.contains("{basin}")) {
            endpoint = endpoint.replace("{basin}", basin);
            this.basinHeader = null;
        } else {
            this.basinHeader = basin;
        }
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (!endpoint.endsWith("/v1")) {
            endpoint = endpoint + "/v1";
        }
        this.baseUri = URI.create(endpoint);
        this.authorization = "Bearer " + token;
    }

    public boolean isTls() {
        return "https".equalsIgnoreCase(baseUri.getScheme());
    }

    public int port() {
        int port = baseUri.getPort();
        if (port != -1) {
            return port;
        }
        return isTls() ? 443 : 80;
    }

    public String recordsPath(String stream) {
        return baseUri.getPath() + "/streams/" + pathEscape(stream) + "/records";
    }

    public URI streamUri(String stream) {
        return baseUri.resolve(baseUri.getPath() + "/streams/" + pathEscape(stream));
    }

    public URI recordsUri(String stream, String query) {
        String uri = baseUri + "/streams/" + pathEscape(stream) + "/records";
        if (query != null && !query.isEmpty()) {
            uri = uri + "?" + query;
        }
        return URI.create(uri);
    }

    static String pathEscape(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
