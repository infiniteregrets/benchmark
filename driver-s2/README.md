# OpenMessaging Benchmark driver for S2

This driver benchmarks [S2](https://s2.dev), the durable streams API, over its
v1 HTTP protocol. The hot paths use `s2s/proto` sessions (framed protobuf over
HTTP/2): pipelined bidirectional append sessions and server-push read
sessions. Stream management uses the unary JSON API.

## Mapping

|       OMB concept       |                                                   S2                                                    |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| Topic with N partitions | N streams named `{topic}/p/{00000..N-1}`                                                                |
| Producer                | One append session per partition stream, batching under the 1000 record / 1 MiB batch limits            |
| Message key             | Hashed to a partition; keyless messages round-robin                                                     |
| Subscription            | Claim stream `{topic}/g/{subscription}`; the total order of claim records assigns partitions to members |
| Publish timestamp       | S2 record timestamp (`client-prefer` timestamping), read back on consume for end-to-end latency         |

S2 has no server-side consumer groups, and OMB scatters the consumers of a
subscription across workers, so group membership is agreed through S2 itself:
each consumer appends a claim record to the subscription's claim stream and
tails it. Because every member observes claims in the same order, member k of
n deterministically owns the partitions where `partition % n == k`, rebalancing
as members join (which only happens during setup, before load starts).

## Running

Create a basin, then:

```bash
export S2_ACCESS_TOKEN=...
bin/benchmark \
  --drivers driver-s2/s2.yaml \
  workloads/1-topic-1-partition-100b.yaml
```

Set `basin` (and optionally `basinEndpoint`) in `driver-s2/s2.yaml`. A
placeholder-free endpoint such as `basinEndpoint: http://localhost:8080`
switches the driver to `s2-basin` header addressing, and plain http
endpoints use HTTP/2 cleartext.
