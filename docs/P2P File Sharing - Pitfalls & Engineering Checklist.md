P2P File Sharing: Key Pitfalls & Things to Keep in Mind

System Architecture, Network Protocols, & Concurrency Engineering Checklist

1. Protocol & Socket Framing

Strict Framing Boundaries on Persistent Sockets: When switching from a 4-byte length-prefixed JSON header (STREAM_RES) to raw binary bytes on the same persistent TCP socket, the receiver must read strictly length bytes using a remaining-byte counter loop (never reading until EOF / -1). Reading even 1 byte too far or too short will corrupt the next JSON control frame on that reused socket.

TCP_NODELAY & Socket Timeouts: Enable socket.setTcpNoDelay(true) on persistent control and data sockets so small HELLO, SEARCH_REQ, and STREAM_REQ headers are sent immediately without Nagle's algorithm delays. Set an explicit read timeout (SO_TIMEOUT, e.g., 5–10s) on stream sockets so a silently disconnected seeder triggers work-stealing failover instead of hanging a worker thread forever.

Clean Connection Pool Eviction: Whenever a pooled socket throws IOException or SocketTimeoutException, close it, remove it from the pool, and evict the peer from SwarmDirectoryCache before re-queuing the failed ByteRange.

2. Concurrent Disk I/O & File Integrity

Positional FileChannel Writes Only: Always use FileChannel.write(ByteBuffer, long position) for parallel workers—never the cursor-based FileChannel.write(ByteBuffer)—so concurrent threads never race on a shared file position cursor.

Safe Temporary File Lifecycle: Write incoming streams into a .part temporary filename (e.g., movie.mp4.part) and keep the destination FileChannel open until all workers finish. Call channel.force(true) before closing, verify the whole-file SHA-256 hash, and only then atomically rename .part to the final filename.

Handle Unrecoverable Failures Cleanly: If a download is cancelled or all seeders drop permanently, close the FileChannel and either retain .part + bytes_downloaded for resume capabilities, or delete the .part file entirely so partial corrupt files never look complete to the system.

3. Peer Discovery & Cache Consistency

Self-Node Filtering: During subnet and port-range scans, explicitly ignore the local node's own peerId and (localIP, listenPort) tuple so the node never discovers, registers, or queries itself in an infinite loop.

Cross-Check Cache Hits Against Live Swarm State: Because QueryCache uses a 60-second sliding TTL, always verify on a cache hit that at least one hosting peer for each returned file is still marked ONLINE in SwarmDirectoryCache so you never serve dead results to clients.

Thread-Safe Cache Structures: Use ConcurrentHashMap and ConcurrentSkipListSet (or CopyOnWriteArraySet) for SwarmDirectoryCache and QueryCache so background 15s heartbeat sweeps, search responses, and download workers never trigger a ConcurrentModificationException.

4. SQLite Concurrency & Performance

Setting / Metric

Configuration Value

Target Behavior & Rationale

Journal Mode

PRAGMA journal_mode=WAL;

Enables Write-Ahead Logging to allow concurrent reads while writes are occurring.

Busy Timeout

PRAGMA busy_timeout=5000;

Prevents SQLITE_BUSY lock errors during concurrent scan, sweep, and download updates.

Progress Flushing

In-Memory AtomicLong

Avoids writing bytes_downloaded per 64 KB read. Checkpoint to DB only per 4 MB window.

Enable WAL Mode & Busy Timeout: Because the concurrent port scanner (32–64 threads), 15s heartbeat sweep, and parallel download workers all update peers and downloads tables simultaneously, execute the PRAGMA statements above in DatabaseInitializer.java.

Avoid Per-Packet DB Writes: Never write bytes_downloaded to SQLite on every 64 KB buffer read. Track live progress in an AtomicLong in RAM for SSE/WebSocket UI updates, and flush checkpoints to SQLite only per completed 4 MB window or upon pause/failure events.

5. Parallelism, Memory & Network Constraints

Small-File Threshold & Single-Host Lane Cap: Skip parallel window splitting for files under 4 MB (setup overhead outweighs gain), and cap single-host parallel lanes at 4 sockets so you saturate Wi-Fi/TCP windows without causing disk seek thrashing on the seeding host.

Flat Memory Footprint: Reuse a single 64 KB direct buffer (ByteBuffer.allocateDirect(65536)) per worker thread instead of allocating 4 MB byte arrays per virtual window, keeping JVM heap usage flat during 1 GB+ transfers.

Firewalls, AP Isolation & File Descriptor Limits: Campus or public Wi-Fi networks often enable client/AP isolation that blocks peer-to-peer subnet probes. Always keep 127.0.0.1 multi-port scanning (9000–9005) and a manual peer-registration fallback ready for live demos. Bound the scanner pool with a 300 ms connect timeout so probing 254 IPs does not exhaust OS file descriptors (ulimit -n).

6. Quick Pre-Demo Verification Checklist

Verify QueryCache hit rate and < 1 ms response time on repeated searches via GET /api/cache/stats.

Confirm zero .chunk files are created on disk during both upload registration and parallel download phases.

Test killing 1 seeder mid-transfer (at ~50% completion) and confirm the remaining lane(s) steal the interrupted ByteRange, finish writing the file, and pass SHA-256 verification.