# Distributed Peer Search and File Sharing Engine

An implementation of a decentralized peer-to-peer (P2P) file sharing and discovery network. The system enables autonomous peer nodes to index local files, partition them into fixed-size chunks, execute distributed keyword searches across the network overlay without a centralized server, and transfer data concurrently over raw TCP sockets.

---

## 1. Problem Statement

Centralized client-server architectures introduce single points of failure, network bottlenecks at central servers, and high infrastructure overhead. In addition, centralized index servers pose scalability limits for file discovery in distributed networks.

**Objectives of this Project:**

* Eliminate reliance on central coordinating servers or trackers.


* Implement a decentralized search mechanism using Time-To-Live (TTL) bounded flooding with duplicate suppression to locate files across arbitrary network topologies.


* Partition shared files into discrete, fixed-size chunks (1 MB) to allow parallel acquisition from multiple peers and fault-tolerant transfers.


* Ensure complete file reconstruction fidelity through sequential byte verification.


* Provide a responsive web dashboard for node management, active connection monitoring, and real-time chunk transfer visualization.



---

## 2. Technical Stack and Design Decisions

| Layer / Subsystem | Technology | Rationale |
| --- | --- | --- |
| **Peer Node Runtime** | **Java 17+ / Spring Boot 3**<br> | High-concurrency multithreading, robust networking libraries, and production-grade REST/WebSocket API capabilities.

 |
| **P2P Transport Protocol** | **Raw TCP Sockets**<br> | Direct node-to-node transport utilizing custom 4-byte length-prefixed framing to eliminate packet boundary concatenation and framing errors.

 |
| **Metadata Persistence** | **SQLite (via SQLite-JDBC)**<br> | Lightweight, zero-configuration relational storage for tracking file indices, peer records, and chunk states without external database server overhead.

 |
| **User Interface** | **React / TypeScript (Vite)**<br> | Decoupled client dashboard interfacing with the local peer node through standard REST and WebSocket protocols.

 |

### Architectural Boundary Constraints

To maintain conceptual clarity and focus on fundamental networking and operating system principles, the system design deliberately avoids external distributed middleware:

* No centralized message brokers or distributed caches (e.g., Kafka, Redis).


* No Distributed Hash Tables (DHT) or third-party discovery libraries (e.g., Kademlia, WebSockets for P2P transport).


* No full-text search index engines (e.g., Elasticsearch, Lucene); all local index lookups execute via structured relational queries.



---

## 3. System Architecture

Each peer operates as a self-contained node structured around four core processing engines:

```
+-------------------------------------------------------------+
|                     React Web Dashboard                     |
+-------------------------------------------------------------+
                              |
               (REST API / WebSocket Events)
                              v
+-------------------------------------------------------------+
|                    Spring Boot Peer Node                    |
|                                                             |
|   +-----------------------+     +-----------------------+   |
|   |  E1: Network Manager  |     |   E2: Search Manager  |   |
|   |  (Sockets & Framing)  |     | (Flooding & De-dupe)  |   |
|   +-----------------------+     +-----------------------+   |
|                                                             |
|   +-----------------------+     +-----------------------+   |
|   |  E3: File & Chunk Mgr |     |  E4: Download Manager |   |
|   |  (Chunking & Assembly)|     |  (State Machine & RX) |   |
|   +-----------------------+     +-----------------------+   |
|               |                             |               |
|      (Relational Data)                 (File I/O)           |
|               v                             v               |
|         [ SQLite DB ]             [ Local File Storage ]    |
+-------------------------------------------------------------+
            ^                                     ^
            | (Raw TCP with Length Prefix)        |
            v                                     v
     [ Remote Peer A ]                     [ Remote Peer B ][cite: 4, 8]

```

### Core Engine Responsibilities

* **E1 Network Manager:** Manages inbound TCP server listeners, outbound peer socket connections, length-prefixed frame encoding/decoding, and connection lifecycles.


* **E2 Search Manager:** Handles search query routing, TTL decrementing, request UUID tracking to suppress cycles, and local database file matching (`LIKE '%query%'`).


* **E3 File & Chunk Manager:** Splits local files into 1 MB binary chunks (`peer-data/chunks/<fileId>/...`), maintains chunk disk indexes, and reassembles received chunks into original files.


* **E4 Download Manager:** Manages transfer state machines, chunk requests (`PENDING`, `DOWNLOADING`, `COMPLETED`, `FAILED`), peer chunk assignments, and retry strategies.



---

## 4. Database Schema (SQLite)

Relational metadata is decoupled from raw file data. The database maintains peer and file metadata, while all binary payload chunks are stored directly on the filesystem.

```sql
-- Known peers in the overlay network
CREATE TABLE peers (
    peer_id TEXT PRIMARY KEY,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    status TEXT NOT NULL
);[cite: 6]

-- Registered files hosted or known by this node
CREATE TABLE files (
    file_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    owner_peer_id TEXT NOT NULL,
    original_path TEXT NOT NULL,
    FOREIGN KEY (owner_peer_id) REFERENCES peers (peer_id)
);[cite: 6]

-- Individual chunk indices and storage paths
CREATE TABLE chunks (
    chunk_id TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    path TEXT NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(file_id),
    UNIQUE(file_id, chunk_no)
);[cite: 6]

-- Mapping of peer availability per file
CREATE TABLE file_peers (
    file_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    PRIMARY KEY(file_id, peer_id)
);[cite: 6]

```

---

## 5. Current Implementation Progress

### Completed Milestones

* **Phase 0: Project Setup & Baseline**
* Configured project directory structure and version control rules in `.gitignore` (excluding `.db`, runtime chunk directories, and build artifacts).
* Defined explicit scope constraints and architectural invariants in `AGENTS.md`.


* **Phase 1: Backend Foundation**
* Initialized Spring Boot backend configured for node operation on port `8081`.


* Implemented and verified baseline health check controller (`GET /api/health`).


* **Phase 2: Database Layer & Schema Initialization**
* Integrated `sqlite-jdbc` driver.
* Implemented `DatabaseInitializer` to automatically verify and create relational schema tables (`peers`, `files`, `chunks`, `file_peers`) at boot time.





---

## 6. Implementation Roadmap

* **Phase 3: File and Chunk Management (E3)**
* Implement 1 MB chunking logic via Java NIO and file reassembly with byte-level verification.




* **Phase 4 & 5: TCP Transport and Handshake Protocols (E1)**
* Implement length-prefixed raw TCP framing and peer discovery handshakes (`HELLO`, `PEER_LIST`).




* **Phase 6 & 7: Distributed Search & Chunk Transfer Engine (E2, E4)**
* Implement flooding search with TTL bounds, cyclic query suppression, and chunk request/response pipelines (`CHUNK_REQUEST`, `CHUNK_RESPONSE`).




* **Phase 8 & 9: Web API & Dashboard Interface**
* Expose node management endpoints and real-time WebSocket progress streams; build the React web dashboard.


* **Phase 10 – 12: Multi-Peer Swarm Transfer & Fault Verification**
* Implement concurrent chunk retrieval across multiple peers, verify node drop recovery, and validate on a 3-node local topology.





---

## 7. Execution and Verification

### Prerequisites

* JDK 17 or higher
* Apache Maven 3.8+

### Running the Peer Node

From the `backend` directory:

```bash
./mvnw spring-boot:run

```

*(On Windows Command Prompt: `mvnw.cmd spring-boot:run`)*

### Validating Service Health

```bash
curl http://localhost:8081/api/health

```

Expected response:

```json
{"service":"P2P Peer Node","status":"UP"}

```
