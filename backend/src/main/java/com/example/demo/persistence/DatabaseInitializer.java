package com.example.demo.persistence;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@Component
public class DatabaseInitializer {

    @Value("${db.url:jdbc:sqlite:peer.db}")
    private String dbUrl;

    @PostConstruct
    public void init() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            // Enable WAL mode and busy timeout for high-concurrency access
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA busy_timeout=5000;");

            // 1. peers table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS peers (
                    peer_id TEXT PRIMARY KEY,
                    host TEXT NOT NULL DEFAULT '127.0.0.1',
                    ip TEXT NOT NULL DEFAULT '127.0.0.1',
                    port INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'CONNECTED',
                    rtt_ms INTEGER DEFAULT 0,
                    share_ready BOOLEAN DEFAULT 1,
                    data_ports TEXT,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """);
            ensurePeersColumns(conn);

            // 2. files table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    owner_peer_id TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    file_hash TEXT
                );
            """);
            ensureFilesColumns(conn);

            // 3. downloads table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS downloads (
                    file_id TEXT PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    total_size BIGINT NOT NULL,
                    bytes_downloaded BIGINT DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """);
            ensureDownloadsColumns(conn);

            // 4. chunks table (legacy backwards compatibility)
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    chunk_no INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    FOREIGN KEY (file_id) REFERENCES files(file_id),
                    UNIQUE(file_id, chunk_no)
                );
            """);

            // 5. file_peers table (legacy backwards compatibility)
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS file_peers (
                    file_id TEXT NOT NULL,
                    peer_id TEXT NOT NULL,
                    PRIMARY KEY(file_id, peer_id)
                );
            """);

            System.out.println(">>> SQLite metadata tables and pragmas initialized successfully (WAL mode enabled).");

        } catch (SQLException e) {
            System.err.println("Failed to initialize SQLite tables: " + e.getMessage());
        }
    }

    private Set<String> getExistingColumns(Connection conn, String tableName) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        return columns;
    }

    private void ensurePeersColumns(Connection conn) throws SQLException {
        Set<String> cols = getExistingColumns(conn, "peers");
        try (Statement stmt = conn.createStatement()) {
            if (!cols.contains("host")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN host TEXT NOT NULL DEFAULT '127.0.0.1';");
            }
            if (!cols.contains("ip")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN ip TEXT NOT NULL DEFAULT '127.0.0.1';");
            }
            if (!cols.contains("port")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN port INTEGER NOT NULL DEFAULT 9001;");
            }
            if (!cols.contains("status")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN status TEXT NOT NULL DEFAULT 'CONNECTED';");
            }
            if (!cols.contains("rtt_ms")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN rtt_ms INTEGER DEFAULT 0;");
            }
            if (!cols.contains("share_ready")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN share_ready BOOLEAN DEFAULT 1;");
            }
            if (!cols.contains("data_ports")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN data_ports TEXT;");
            }
            if (!cols.contains("last_seen")) {
                stmt.execute("ALTER TABLE peers ADD COLUMN last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP;");
            }
        }
    }

    private void ensureFilesColumns(Connection conn) throws SQLException {
        Set<String> cols = getExistingColumns(conn, "files");
        try (Statement stmt = conn.createStatement()) {
            if (!cols.contains("file_hash")) {
                stmt.execute("ALTER TABLE files ADD COLUMN file_hash TEXT;");
            }
        }
    }

    private void ensureDownloadsColumns(Connection conn) throws SQLException {
        Set<String> cols = getExistingColumns(conn, "downloads");
        try (Statement stmt = conn.createStatement()) {
            if (!cols.contains("file_name")) {
                stmt.execute("ALTER TABLE downloads ADD COLUMN file_name TEXT NOT NULL DEFAULT '';");
            }
            if (!cols.contains("total_size")) {
                stmt.execute("ALTER TABLE downloads ADD COLUMN total_size BIGINT NOT NULL DEFAULT 0;");
            }
            if (!cols.contains("bytes_downloaded")) {
                stmt.execute("ALTER TABLE downloads ADD COLUMN bytes_downloaded BIGINT DEFAULT 0;");
            }
            if (!cols.contains("status")) {
                stmt.execute("ALTER TABLE downloads ADD COLUMN status TEXT NOT NULL DEFAULT 'PENDING';");
            }
            if (!cols.contains("last_updated")) {
                stmt.execute("ALTER TABLE downloads ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;");
            }
        }
    }
}
