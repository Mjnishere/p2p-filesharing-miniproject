Implementation Plan: Low-Latency Distributed P2P Architecture (Zero-Chunking)

This document details the architectural transition from legacy chunked, base64-encoded transfers to a zero-chunking, direct-offset streaming peer-to-peer subsystem.

1. Objectives & Architectural Shift

Eliminate Transfer & Serialization Delays: Fully remove .chunk disk splitting/reassembly, Base64-in-JSON payload inflation (+33% bloat), and artificial Thread.sleep(100) rate-pacing loops.

Targeted Port-Handshake Peer Discovery: Automatically discover and verify sharing-ready peers via concurrent TCP port probing and application-level HELLO / HELLO_ACK handshakes with live RTT measurement.

Master Node Two-Tier Cache Layer: Reuse prior query results (O(1) lookup) and maintain a live fileHash-to-Peers swarm directory to eliminate repeated discovery and network flooding latency.

Dynamic Work-Stealing Parallel Downloads: Stream virtual 4 MB byte windows concurrently across multiple peers—or across 4 parallel TCP lanes when only a single host has the file—writing directly to non-overlapping offsets via FileChannel.

2. Phase 1: Automatic Peer Discovery (Targeted Port Probe + Handshake)

Interface & Target Port Matrix

Enumerate active non-loopback IPv4 interfaces (e.g., 192.168.1.x/24 subnets) alongside 127.0.0.1 for local multi-instance testing.

Target a configurable port list (default 9000, or 9000–9005 for single-host multi-node testing).

Concurrent TCP Port Probing

Utilize a bounded ExecutorService (32–64 threads) to perform fast TCP connection probes using Socket.connect(addr, 300ms).

Application-Level HELLO / HELLO_ACK Verification

Immediately send a 4-byte length-prefixed JSON frame containing discovery payloads.

Measure precise RTT: rttMs = (System.nanoTime() - startNs) / 1_000_000.

// HELLO Request Payload

{

  "type": "HELLO",

  "peerId": "node-alpha-99",

  "listenPort": 9000,

  "dataPorts": [9001, 9002, 9003, 9004],

  "shareReady": true

}

// HELLO_ACK Response Payload

{

  "type": "HELLO_ACK",

  "status": "AVAILABLE",

  "peerId": "node-beta-01",

  "shareReady": true

}

Peer Registration & 15s Heartbeat Pruning

Upsert verified peers into the peers database table and in-memory active map with fields rtt_ms and share_ready = 1.

Execute a scheduled 15-second sweep to re-probe known peers, update rolling RTT averages, and mark unresponsive peers OFFLINE (evicting them from active swarm caches).

3. Phase 2: Master Node Two-Tier Cache Layer

Tier 1 — QueryCache (Search Result Reuse)

ConcurrentHashMap<String, CachedQueryResult> bounded LRU cache (maximum 500 entries) keyed by normalized query strings (query.trim().toLowerCase()).

Tracks results, createdAtMs, lastHitAtMs, and hitCount with a 60-second sliding TTL.

Cache Hit Processing (< 0.5 ms): Verifies at least one hosting peer per file remains ONLINE in SwarmDirectoryCache and returns instantly without network RTT. Supports local prefix filtering (e.g., filtering cached "ubuntu" results for "ubuntu 24").

Tier 2 — SwarmDirectoryCache (File-to-Peer Swarm Mapping)

ConcurrentHashMap<String, ConcurrentSkipListSet<PeerEndpoint>> keyed by whole-file SHA-256 hash (fileHash), ordered by lowest rttMs.

Populated passively from incoming SEARCH_RES frames, invalidated on 15s heartbeat failures, and updated via write-through failure eviction if a download worker encounters a socket error.

4. Phase 3: Zero-Chunking Binary Stream Protocol

Protocol Specification

Uses a Hybrid Control Header + Raw Binary Stream over persistent TCP sockets with TCP_NODELAY = true and 64 KB send/receive buffers.

// STREAM_REQ Frame

{

  "type": "STREAM_REQ",

  "fileId": "f-10492",

  "fileHash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",

  "offset": 4194304,

  "length": 4194304

}

// STREAM_RES Frame

{

  "type": "STREAM_RES",

  "status": "OK",

  "fileId": "f-10492",

  "offset": 4194304,

  "length": 4194304

}



Immediately following the STREAM_RES JSON header, exactly length raw binary bytes are written directly onto the stream.

Direct Positional Storage (FileChannel)

Sender: Reads byte range [offset, offset + length) directly from original_path using FileChannel.transferTo or zero-allocation byte buffers.

Receiver: Pre-allocates destination storage once via FileChannel.truncate(fileSize) and streams incoming bytes directly to target byte offsets via thread-safe FileChannel.write(ByteBuffer, currentOffset) (OS pwrite). This eliminates temporary .chunk files and post-download assembly overhead.

5. Phase 4: Dynamic Work-Stealing Virtual Windows

Window Queue Management

Logically partitions the total fileSize in memory into 4 MB virtual range objects (ByteRange(offset, length)) managed within a thread-safe ConcurrentLinkedQueue<ByteRange>.

Adaptive Worker Lane Allocation

Multi-Peer Swarm (M ≥ 2 peers): Spawns 1–2 worker threads per active peer (capped at 6 max total workers), prioritized by lowest measured rttMs.

Single-Host Multi-Lane Parallelism (M = 1 peer, fileSize > 4 MB): Spawns 4 concurrent worker threads targeting the single host. Operates either by initializing 4 parallel Socket connections to peerIP:9000 (establishing independent TCP congestion windows) or by connecting across dedicated data ports (9001–9004) advertised during handshake.

Work-Stealing Loop & Failover Architecture

                       ┌───────────────────────────────┐

                       │  ConcurrentLinkedQueue        │

                       │  [4MB] [4MB] [4MB] [4MB] ...  │

                       └──────────────┬────────────────┘

                                      │

              ┌───────────────────────┼───────────────────────┐

              ▼                       ▼                       ▼

      ┌───────────────┐       ┌───────────────┐       ┌───────────────┐

      │ Worker Lane 1 │       │ Worker Lane 2 │       │ Worker Lane 3 │

      │ (Peer A / TCP)│       │ (Peer B / TCP)│       │ (Peer A / TCP)│

      └───────┬───────┘       └───────┬───────┘       └───────┬───────┘

              │                       │                       │

              └───────────────────────┼───────────────────────┘

                                      ▼

                      ┌───────────────────────────────┐

                      │ Shared FileChannel (pwrite)   │

                      │ Offset-based Direct Insertion │

                      └───────────────────────────────┘



Each worker polls a ByteRange from the queue, transmits a STREAM_REQ, and streams data directly into the shared FileChannel at range.offset + bytesReadSoFar.

Real-time updates to an AtomicLong progress counter drive UI status without lock contention.

Automatic Failover: If a network error occurs mid-transfer, the uncompleted sub-range [offset + bytesRead, remaining] is pushed back to the head of workQueue. The failing peer is marked OFFLINE and evicted from SwarmDirectoryCache. Active remaining workers steal and execute the remaining task.

Completes with a streaming whole-file SHA-256 integrity verification step.

6. File-by-File Backend Execution Plan

File

Changes Required

persistence/DatabaseInitializer.java

Add rtt_ms (INTEGER), share_ready (BOOLEAN), data_ports (TEXT) columns to peers schema. Add file_hash (TEXT) to files table and bytes_downloaded (BIGINT) to downloads table.

FramingUtil.java

Retain 4-byte length-prefixed JSON framing for control headers; add low-level pipeRawBytes(InputStream, FileChannel, offset, length, onProgress) method for direct socket-to-disk streaming.

NetworkService.java

Implement scanSubnetAndPorts using concurrent socket probing + HELLO/HELLO_ACK exchange with RTT calculation. Maintain a persistent TCP socket connection pool; add handleStreamRequest and streamRangeToChannel handlers.

service/SearchManagerService.java

Integrate QueryCache (60s sliding TTL, LRU) and SwarmDirectoryCache. Trigger subnet scanning on startup and every 15s. Replace sequential peer SEARCH_REQ loops with parallel CompletableFuture.allOf.

service/ChunkManagerService.java

Deprecate physical .chunk file creation, temp directory management, and merge functions. Compute whole-file SHA-256 hashes upon file registration and serve arbitrary (offset, length) byte ranges using FileChannel.

service/DownloadManagerService.java

Remove Thread.sleep(100) artificially throttled loops. Implement ConcurrentLinkedQueue<ByteRange> work queue with 4 MB windowing, adaptive worker allocation (multi-peer vs 4-lane single-host), work-stealing failover handling, and post-transfer SHA-256 verification.

controller/FileSharingController.java

Expose POST /api/peers/discover endpoint for manual network scans and GET /api/cache/stats returning cache hit/miss counters, active swarms, and live peer RTT metrics.

7. Verification & Demo Benchmarks to Capture

Discovery Timing

Measure total latency to discover and verify 3+ local/LAN peers using automated TCP port-handshake probing vs. legacy manual IP registration.

Cache Hit vs. Miss Latency

Benchmark SEARCH_REQ full network round-trip miss latency (~20–200 ms) against QueryCache local hit latency (< 0.5 ms).

Transfer Throughput & Zero-Chunk Overhead

Execute end-to-end transfers of 100 MB+ files. Record MB/s throughput, CPU utilization, and total elapsed time comparing legacy chunked transfers against zero-chunking direct-offset streaming (both single-host 4-lane and multi-peer configurations).

Mid-Transfer Failover Verification

Force-terminate one seeding peer process at 50% download progress. Verify that surviving worker lanes steal the interrupted ByteRange, complete the transfer without data corruption, and pass the final whole-file SHA-256 checksum check.