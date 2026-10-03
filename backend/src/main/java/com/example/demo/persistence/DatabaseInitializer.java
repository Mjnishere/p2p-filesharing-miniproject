package com.example.demo.persistence;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class DatabaseInitializer {

    @Value("${db.url}")
private String dbUrl;

    @PostConstruct
    public void init() {
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            // 1. peers table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS peers (
                    peer_id TEXT PRIMARY KEY,
                    ip TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    status TEXT NOT NULL
                );
            """);

            // 2. files table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    owner_peer_id TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    FOREIGN KEY (owner_peer_id) REFERENCES peers (peer_id)
                );
            """);

            // 3. chunks table
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

            // 4. file_peers table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS file_peers (
                    file_id TEXT NOT NULL,
                    peer_id TEXT NOT NULL,
                    PRIMARY KEY(file_id, peer_id)
                );
            """);

            System.out.println(">>> SQLite metadata tables initialized successfully.");

        } catch (SQLException e) {
            System.err.println("Failed to initialize SQLite tables: " + e.getMessage());
        }
    }
}
