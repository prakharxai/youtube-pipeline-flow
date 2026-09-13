# 🌐 Political YouTube Monitor — Interactive Pipeline Architecture

<p align="center">
  <a href="https://prakharxai.github.io/youtube-pipeline-flow/">
    <img src="https://img.shields.io/badge/GitHub%20Pages-Live%20Demo-success?style=for-the-badge&logo=github" alt="Live Demo" />
  </a>
  <img src="https://img.shields.io/badge/Pure-HTML5%20%7C%20CSS3%20%7C%20SVG-orange?style=for-the-badge&logo=html5" alt="Tech" />
  <img src="https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Vanilla%20JS)-blue?style=for-the-badge" alt="Zero Dependencies" />
</p>

### 🚀 [Click Here to View Live Interactive Architecture](https://prakharxai.github.io/youtube-pipeline-flow/)

---

## Overview

This repository hosts the **Interactive System Architecture & Pipeline Flow Visualizer** for the Political YouTube Channel Activity Monitor.

Built as a lightweight, zero-dependency, pure client-side web application featuring dynamic interactive SVG diagrams, stage search, clickable component inspections, and detailed data structures.

---

## 🧭 Interactive Views Included

1. **🌐 Complete Pipeline Swimlanes (End-to-End)**
   - Maps the end-to-end ingestion lifecycle across all stages: Discovery &rarr; Ingestion &rarr; Speech-to-Text &rarr; LLM Synthesis &rarr; Evidence Grounding &rarr; File-First Reports.
2. **🔀 4 Distinct Activity Flows**
   - Detailed swimlanes for Long-Form Videos, YouTube Shorts, Community Posts (audio-free), and YouTube Live streams.
3. **🎯 Grounded Citations & Provenance Flow**
   - Visualizes how claims, quotes, and summaries are grounded against verbatim speech transcripts using fuzzy Levenshtein distance matching and timestamp anchor generation.
4. **⏱️ Processing-Time Profiler**
   - Architectural diagram of wall-clock instrumentation, phase timers, and hardware telemetry tracking across stages.
5. **📁 File-First Storage Tree**
   - Interactive directory hierarchy showcasing predictable NVMe disk storage structures under `data/`, `state/`, and `logs/`.
6. **🛡️ Safe Deletion & Audit Lifecycle**
   - Interactive flow demonstrating 2-step deletion impact preview, cryptographic confirmation tokens (`CONFIRM_DELETE`), and append-only audit logging.

---

## 💻 Running Locally

No installation or build tools required:

```bash
# Option 1: Direct browser launch
open index.html # On macOS
xdg-open index.html # On Linux

# Option 2: Local HTTP server
python3 -m http.server 8080
```
Then visit `http://localhost:8080`.
