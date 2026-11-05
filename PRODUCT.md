# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Embedded HTML5/CSS3/Vanilla JavaScript single-page web view served directly by Spring Boot static resources (`backend/src/main/resources/static/index.html`) with zero external build dependencies.

## Users

Developers, distributed systems students, and peer node operators running local or network P2P instances who need to share files, monitor peer connectivity, execute distributed searches across the network, and track chunk-by-chunk download and reassembly progress.

## Product Purpose

Provide an intuitive, clean, and reliable control surface for a P2P chunk-based file sharing node. Eliminates the need for manual curl/Postman commands by giving users visual file sharing, network discovery, distributed search queries with TTL hops, and real-time chunk download/recovery tracking.

## Positioning

A lightweight, zero-dependency peer node console specifically tailored for chunk-based P2P file distribution with SQLite metadata, TCP framing protocols, and self-healing chunk recovery mechanisms.

## Operating Context

Runs alongside the Spring Boot peer node process on port 8081 (or configured peer port), directly in any modern desktop or mobile browser. Communicates with local REST endpoints (`/api/files`, `/api/search`, `/api/download/*`, `/api/peers`, `/api/health`).

## Capabilities and Constraints

- Capabilities:
  - Local file sharing: split files into 1MB chunks and register in SQLite
  - Peer management: view active connected peers, register new peer hosts/ports
  - Distributed search: query files across peer mesh with TTL propagation and loop suppression
  - Download & reassembly: trigger chunk downloads from peers, visualize per-chunk states (PENDING, REQUESTED, DOWNLOADING, COMPLETED, FAILED), monitor reassembly
  - Test suite / diagnostics: run chunk recovery and health checks directly from UI
- Constraints:
  - Simple HTML web view: no complex npm/node build toolchain needed; runs directly from Spring Boot `resources/static/`
  - High performance, clean typography, responsive layout, light/dark mode support
  - Clear feedback for peer connection states and download retries

## Brand Commitments

- Aesthetic: Clean Minimalist File Manager
- Tone: Crisp, distraction-free, functional, high-density utility with subtle refined micro-interactions and dual light/dark theme

## Evidence on Hand

- Spring Boot backend with REST APIs in `com.example.demo.controller.FileSharingController`:
  - `GET /api/files`
  - `POST /api/files/share`
  - `GET /api/search?q={}&ttl={}`
  - `POST /api/download/start`
  - `GET /api/download/status?fileId={}`
  - `GET /api/download/all`
  - `GET /api/peers`
  - `POST /api/peers/register`
  - `GET /api/health`
  - `GET /api/test/chunk-recovery`
- SQLite schema with tables `peers`, `files`, `chunks`, `file_peers`.

## Product Principles

1. Zero Overhead: Single cohesive web view served straight from the backend with no build step required.
2. Transparency of P2P State: Make peer connectivity, file chunks, and network hops tangible and immediately readable.
3. Resilient Feedback: Show chunk retry counts, failure recovery, and transfer speeds gracefully without layout shifts.
4. Minimalist Elegance: Utilitarian whitespace, refined typography, polished light/dark theme, and clear semantic states.
