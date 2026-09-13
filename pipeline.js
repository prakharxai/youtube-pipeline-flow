/**
 * Political YouTube Channel Activity Monitor
 * Standalone Interactive Pipeline Architecture & Flow Documentation
 * Zero external runtime dependencies - Pure Vanilla JS & Dynamic SVG.
 */

// =============================================================================
// Comprehensive Technical Specifications for Every Pipeline Stage
// =============================================================================
const STAGE_SPECS = {
  "watchlist": {
    name: "Channel Watchlist Registry",
    category: "Ingestion Config",
    type: "GLOBAL",
    color: "#3b82f6",
    description: "Loads and validates active YouTube channel configurations from YAML files. Configures channel ID, handle, friendly name, and category tags.",
    inputs: "config/channels.yaml, config/settings.yaml",
    outputs: "List of validated ChannelConfig Pydantic objects",
    filesProduced: "config/channels.yaml, data/channels/{channel_id}/metadata.json",
    timingTrackers: "startup_channel_load (microseconds)",
    failureModes: "YAML parsing syntax error, missing required keys (id, name), inaccessible config path.",
    retryPolicy: "Immediate abort on startup with informative terminal diagnostic log.",
    codeRef: "src/config.py -> Settings.load_channels()",
    samplePayload: {
      "channel_id": "sansad_tv",
      "channel_name": "Sansad TV",
      "url": "https://www.youtube.com/@SansadTV",
      "enabled": true,
      "tags": ["parliament", "policy", "national"]
    }
  },
  "discovery": {
    name: "Activity Discovery & Multi-Tab Scanner",
    category: "Discovery",
    type: "ALL FLOWS",
    color: "#3b82f6",
    description: "Scans YouTube channels across multiple tabs (/videos, /shorts, /community, /streams) using yt-dlp flat-playlist extraction and metadata scraping.",
    inputs: "Channel URL, target date, date range filter, max items limits",
    outputs: "Raw activity metadata entries (video ID, title, publication timestamp, channel metadata)",
    filesProduced: "data/activities/YYYY/MM/DD/{videos,shorts,posts,live}/{item_id}.json",
    timingTrackers: "discovery_start, discovery_end, stage_duration_seconds",
    failureModes: "YouTube rate-limiting / bot detection (HTTP 429), network socket timeout, channel renamed or private.",
    retryPolicy: "Exponential backoff with 3 retries (1s, 2s, 4s delay). Fallback to cached RSS if available.",
    codeRef: "src/engine/collector.py -> ActivityCollector.discover_activities()",
    samplePayload: {
      "activity_type": "VIDEO",
      "item_id": "EvF9mhGyMps",
      "channel_id": "sansad_tv",
      "title": "Aapke Sansad | Dr. Manna Lal Rawat, MP Udaipur | 11 Sept, 2026",
      "published_at": "2026-09-12T05:30:00Z",
      "duration": 1794
    }
  },
  "date_filter": {
    name: "Publication Date Filter & Window Matcher",
    category: "Ingestion Gate",
    type: "ALL FLOWS",
    color: "#06b6d4",
    description: "Strictly filters discovered activities by comparing publication timestamps against the requested UTC single date or date range.",
    inputs: "Raw activity published_at, target_date (YYYY-MM-DD), or from_date -> to_date range",
    outputs: "Filtered list of activities within the target temporal window",
    filesProduced: "In-memory filtered collection passed to deduplication gate",
    timingTrackers: "filter_eval_microseconds",
    failureModes: "Malformed ISO-8601 publication timestamps, timezone offset ambiguities.",
    retryPolicy: "Parses with dateutil.parser; defaults to UTC timezone if unspecified.",
    codeRef: "src/engine/pipeline.py -> MonitoringPipeline._filter_by_date()",
    samplePayload: {
      "target_date": "2026-09-12",
      "item_published_at": "2026-09-12T05:30:00Z",
      "matched": true
    }
  },
  "dedup": {
    name: "Deduplication & State Hash Gate",
    category: "Ingestion Gate",
    type: "IDEMPOTENCY",
    color: "#8b5cf6",
    description: "Guarantees idempotency. Checks state/items/{item_id}.json to determine whether an item has already been successfully analyzed, failed, or requires reprocessing.",
    inputs: "item_id, force_reprocess flag, existing state files",
    outputs: "Boolean gate decision: PROCESS vs SKIP",
    filesProduced: "state/items/{item_id}.json",
    timingTrackers: "dedup_check_microseconds",
    failureModes: "State JSON corrupted or file lock contention during concurrent runs.",
    retryPolicy: "Atomic JSON file write with tempfile rename. Recovers state from disk if corrupted.",
    codeRef: "src/engine/state_manager.py -> StateManager.should_process()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "current_stage": "COMPLETED",
      "first_seen_at": "2026-09-12T23:31:56Z",
      "last_processed_at": "2026-09-12T23:33:14Z",
      "process_count": 1
    }
  },
  "download": {
    name: "Media Download & Audio Extraction",
    category: "Processing",
    type: "VIDEO, SHORT, LIVE",
    color: "#ec4899",
    description: "Downloads the highest efficiency compressed audio stream (Opus / m4a / webm) via yt-dlp to minimize disk I/O and network bandwidth.",
    inputs: "YouTube item URL, target output path, download bitrate flags",
    outputs: "Local compressed audio stream in raw storage",
    filesProduced: "data/raw/{item_id}/audio.opus",
    timingTrackers: "download_start, download_end, duration_seconds",
    failureModes: "Geo-blocking, content DRM protection, intermittent network disconnects, YouTube bot verification challenge.",
    retryPolicy: "3 retries with exponential backoff and alternate yt-dlp user-agent strings.",
    codeRef: "src/engine/downloader.py -> MediaDownloader.download_audio()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "file_path": "data/raw/EvF9mhGyMps/audio.opus",
      "file_size_bytes": 14592000,
      "audio_codec": "opus",
      "duration_seconds": 1794
    }
  },
  "audio_norm": {
    name: "FFmpeg Audio Resampling & Normalization",
    category: "Processing",
    type: "VIDEO, SHORT, LIVE",
    color: "#ec4899",
    description: "Converts the raw audio stream to standard 16kHz mono 16-bit PCM WAV using FFmpeg for optimal input to faster-whisper and Silero VAD.",
    inputs: "data/raw/{item_id}/audio.opus",
    outputs: "data/raw/{item_id}/audio_16k.wav",
    filesProduced: "data/raw/{item_id}/audio_16k.wav",
    timingTrackers: "audio_extraction_start, audio_extraction_end, duration_seconds",
    failureModes: "FFmpeg binary missing from PATH, corrupted container headers, audio stream missing channels.",
    retryPolicy: "Immediate failure logged with stderr capture; cleans up intermediate temporary files.",
    codeRef: "src/engine/downloader.py -> MediaDownloader.convert_to_wav()",
    samplePayload: {
      "sample_rate": 16000,
      "channels": 1,
      "format": "s16le",
      "duration_seconds": 1794.2
    }
  },
  "post_text": {
    name: "Community Post Text & Media Parser",
    category: "Processing",
    type: "COMMUNITY POST",
    color: "#8b5cf6",
    description: "Extracts community post body text, attached image carousels, poll options, and external hyperlinks without requiring audio extraction or speech-to-text.",
    inputs: "Community post URL, post element HTML or InnerTube API response",
    outputs: "Extracted post content, image attachment URLs, and status code",
    filesProduced: "data/activities/YYYY/MM/DD/posts/{item_id}.json",
    timingTrackers: "post_extraction_duration_seconds",
    failureModes: "YouTube UI DOM selector changes, post removed by creator.",
    retryPolicy: "Dual extraction: InnerTube JSON endpoint first, falling back to Playwright headless browser rendering.",
    codeRef: "src/engine/post_collector.py -> PostCollector.extract_post()",
    samplePayload: {
      "item_id": "UgkxyD3sCmEsVAewDTThOKyE_4RhoR_62zib",
      "post_type": "TEXT_WITH_IMAGES",
      "content_length": 420,
      "has_attachments": true,
      "image_urls": ["https://yt3.ggpht.com/..."]
    }
  },
  "live_segmenter": {
    name: "YouTube Live Stream Segmenter",
    category: "Processing",
    type: "LIVE STREAM",
    color: "#ef4444",
    description: "Monitors and captures active or completed YouTube Live streams. Handles live streaming states (active live, was_live, scheduled upcoming) and buffers audio chunks.",
    inputs: "Live broadcast URL, stream status indicator",
    outputs: "Recorded VOD audio stream or incremental segment buffer",
    filesProduced: "data/raw/{item_id}/live_stream.wav",
    timingTrackers: "live_capture_duration_seconds",
    failureModes: "Stream terminated abruptly, live encoder disconnect, YouTube livestream latency variations.",
    retryPolicy: "Dynamic reconnection attempts every 5 seconds until stream ends or transitions to VOD.",
    codeRef: "src/engine/live_collector.py -> LiveStreamCollector.capture_stream()",
    samplePayload: {
      "item_id": "MbEP8mEKpRc",
      "live_status": "was_live",
      "stream_duration_sec": 3600,
      "chunks_captured": 12
    }
  },
  "transcription": {
    name: "Faster-Whisper CUDA Speech-to-Text",
    category: "Transcription",
    type: "SPEECH",
    color: "#10b981",
    description: "GPU-accelerated transcription using faster-whisper with float16 compute on NVIDIA RTX GPU. Employs Silero VAD to eliminate non-speech audio and outputs word-level timestamps in Hindi, English, and Hinglish.",
    inputs: "data/raw/{item_id}/audio_16k.wav",
    outputs: "Timestamped transcript segments with verbatim spoken words",
    filesProduced: "data/transcripts/{item_id}.json",
    timingTrackers: "transcription_start, transcription_end, duration_seconds",
    failureModes: "CUDA out-of-memory (OOM), silent/music-only audio track, unintelligible noise.",
    retryPolicy: "Detects music-only content and marks 'NO_SPEECH'; falls back to CPU compute if GPU memory exhausted.",
    codeRef: "src/engine/transcriber.py -> FasterWhisperTranscriber.transcribe()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "language": "hi",
      "segments_count": 142,
      "sample_segment": {
        "start": 25.4,
        "end": 31.8,
        "text": "उदयपुर संसदीय क्षेत्र के सभी विकास कार्यों पर चर्चा करते हुए...",
        "words": [{"word": "उदयपुर", "start": 25.4, "end": 26.1}]
      }
    }
  },
  "llm_analysis": {
    name: "Dual-Engine Ollama LLM Extraction",
    category: "AI Analysis",
    type: "ALL FLOWS",
    color: "#f59e0b",
    description: "Extracts structured political intelligence using local Ollama models. Employs Gemma 3 12B as primary model and Qwen3 8B as fast fallback, with native Ollama JSON Schema grammar enforcement.",
    inputs: "Clean transcript segments / post text, versioned prompt template (config/prompts/)",
    outputs: "Pydantic-validated JSON containing claims, topics, entities, announcements, and executive summary",
    filesProduced: "data/analysis/{item_id}.json",
    timingTrackers: "llm_analysis_start, llm_analysis_end, duration_seconds",
    failureModes: "LLM generation timeout, JSON Schema violation, Ollama daemon disconnect.",
    retryPolicy: "Primary model (gemma3:12b) retry with repair prompt; automatic fallback to Qwen3 8B if primary fails.",
    codeRef: "src/engine/llm_client.py -> OllamaLLMClient.analyze_content()",
    samplePayload: {
      "summary": "Dr. Manna Lal Rawat MP Udaipur highlights tribal development and ₹79,000 crore Pradhan Mantri Janjatiya Uthan Abhiyan.",
      "topics": ["Tribal Welfare", "Infrastructure", "Road Safety", "Water Conservation"],
      "claims_extracted": 13,
      "model_used": "gemma3:12b"
    }
  },
  "evidence_validation": {
    name: "Strict Evidence Grounding & Fuzzy Matching",
    category: "Verification",
    type: "ALL FLOWS",
    color: "#10b981",
    description: "Every claim and summary point extracted by the LLM is mathematically verified against verbatim transcript passages using Levenshtein fuzzy ratio matching (threshold >= 0.75). Hallucinations are rejected.",
    inputs: "data/analysis/{item_id}.json, data/transcripts/{item_id}.json",
    outputs: "Verified claims with exact character and timestamp anchor matches",
    filesProduced: "data/analysis/{item_id}_grounded.json",
    timingTrackers: "grounding_verification_microseconds",
    failureModes: "Claim paraphrase distance exceeds fuzzy threshold.",
    retryPolicy: "Discards ungrounded assertions while preserving only claims with direct verbatim evidence.",
    codeRef: "src/engine/evidence_verifier.py -> EvidenceVerifier.verify_claims()",
    samplePayload: {
      "claim_id": "c-01",
      "statement": "A significant budget of ₹79,000 crore has been allocated for Pradhan Mantri Janjatiya Uthan Abhiyan.",
      "quote": "प्रधानमंत्री जनजातीय उन्नत ग्राम अभियान के तहत 79,000 करोड़ का बजट आवंटित किया गया है",
      "timestamp_start": 348,
      "timestamp_end": 362,
      "fuzzy_ratio": 0.88,
      "grounded": true
    }
  },
  "citation_generation": {
    name: "Deep Citation Anchor Linking",
    category: "Verification",
    type: "ALL FLOWS",
    color: "#06b6d4",
    description: "Generates interactive citation badges ([1], [2]) linking directly to exact video seconds (?t=XXs) or post permalinks, ensuring full auditability for every claim.",
    inputs: "Verified claims with timestamp offsets",
    outputs: "Grounded text with inline citation anchors and clickable URL references",
    filesProduced: "data/analysis/{item_id}_citations.json",
    timingTrackers: "citation_formatting_microseconds",
    failureModes: "Invalid timestamp offset outside video duration.",
    retryPolicy: "Bounds checks against total media duration.",
    codeRef: "src/engine/citation_builder.py -> CitationBuilder.inject_citations()",
    samplePayload: {
      "citation_badge": "[1]",
      "citation_url": "https://www.youtube.com/watch?v=EvF9mhGyMps&t=348s",
      "verbatim_speech": "प्रधानमंत्री जनजातीय उन्नत ग्राम अभियान...",
      "timestamp_display": "05:48"
    }
  },
  "summary_synthesis": {
    name: "Grounded Executive Summary Synthesizer",
    category: "Synthesis",
    type: "ALL FLOWS",
    color: "#3b82f6",
    description: "Synthesizes multi-paragraph executive summaries and key bullet points where every factual sentence is backed by grounded citation indices.",
    inputs: "Verified claims and citation dictionary",
    outputs: "Grounded executive summary and key points with interactive markdown badges",
    filesProduced: "Part of final item and daily report bundles",
    timingTrackers: "synthesis_duration_microseconds",
    failureModes: "Summary contains sentences without verifiable evidence link.",
    retryPolicy: "Strict validation: strips sentences lacking valid citation tag.",
    codeRef: "src/engine/summary_synthesizer.py -> GroundedSummarySynthesizer.build()",
    samplePayload: {
      "executive_summary": "Dr. Manna Lal Rawat MP Udaipur emphasizes tribal welfare initiatives [1]. A budget of ₹79,000 crore has been allocated for rural upliftment [2].",
      "grounded_ratio": 1.0
    }
  },
  "reports": {
    name: "Multi-Format File-First Report Generator",
    category: "Reporting",
    type: "ALL FLOWS",
    color: "#8b5cf6",
    description: "Generates comprehensive item-level and daily rollup reports in HTML, JSON, and Markdown formats. Designed for zero-database, file-first permanence.",
    inputs: "Item intelligence, daily aggregated metrics",
    outputs: "Standalone interactive HTML reports, machine-readable JSONs, and executive Markdown files",
    filesProduced: "data/reports/items/{item_id}.html, data/reports/daily/YYYY-MM-DD.html",
    timingTrackers: "report_generation_duration_seconds",
    failureModes: "Jinja2 HTML template rendering error, disk write permission denied.",
    retryPolicy: "Atomic file writing with temporary file replacement.",
    codeRef: "src/storage/report_generator.py -> ReportGenerator.generate_daily_report()",
    samplePayload: {
      "report_date": "2026-09-12",
      "total_items": 28,
      "channels_included": ["sansad_tv", "_narendramodi"],
      "formats_rendered": ["html", "json", "md"]
    }
  },
  "timing_profiler": {
    name: "Microsecond Processing-Time Profiler",
    category: "Telemetry",
    type: "ALL FLOWS",
    color: "#f59e0b",
    description: "Profiles wall-clock execution time for every individual stage (download, extraction, transcription, LLM, verification) with microsecond precision using time.perf_counter().",
    inputs: "Stage entry/exit timestamps across pipeline execution",
    outputs: "ActivityTiming records tracking cumulative latency and bottleneck telemetry",
    filesProduced: "data/analytics/timing/{item_id}.json",
    timingTrackers: "stage_timings, total_processing_time_seconds",
    failureModes: "Clock skew or incomplete timing trace on sudden process termination.",
    retryPolicy: "Persists intermediate timing checkpoints after every stage completion.",
    codeRef: "src/engine/profiler.py -> StageTimingProfiler",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "total_duration_sec": 78.4,
      "breakdown": {
        "download_sec": 4.8,
        "audio_norm_sec": 1.2,
        "transcription_sec": 38.5,
        "llm_analysis_sec": 28.6,
        "verification_sec": 0.4
      }
    }
  },
  "analytics_aggregator": {
    name: "Dynamic Date-Range Analytics Aggregator",
    category: "Analytics",
    type: "GLOBAL",
    color: "#06b6d4",
    description: "Aggregates intelligence, channel distributions, topic frequencies, and latency statistics across configurable time windows (Today, 7D, 30D, Custom).",
    inputs: "data/activities/, data/analysis/, data/analytics/timing/",
    outputs: "Aggregated dashboard metrics and chart payloads for React frontend",
    filesProduced: "In-memory caching with periodic disk revalidation",
    timingTrackers: "dashboard_aggregation_microseconds",
    failureModes: "Corrupted activity JSON in storage directory.",
    retryPolicy: "Gracefully skips invalid files and logs warning in audit stream.",
    codeRef: "src/analytics/dashboard_service.py -> DashboardService.get_metrics()",
    samplePayload: {
      "date_range": "2026-09-07 to 2026-09-13",
      "total_activities": 41,
      "videos": 19,
      "shorts": 4,
      "posts": 12,
      "live": 6,
      "total_proc_time": "3h 26m"
    }
  },
  "sse_telemetry": {
    name: "Server-Sent Events (SSE) Live Streamer",
    category: "UI Streaming",
    type: "GLOBAL",
    color: "#10b981",
    description: "Broadcasts live stage transitions, item progress, log messages, and error alerts to connected web UI clients over persistent HTTP SSE connection.",
    inputs: "ProgressEvent bus from MonitoringPipeline",
    outputs: "Real-time SSE event stream at /api/monitor/events",
    filesProduced: "logs/runs/{run_id}/events.jsonl",
    timingTrackers: "event_dispatch_microseconds",
    failureModes: "Client disconnection or slow network subscriber.",
    retryPolicy: "Queue drop protection with thread-safe loop dispatch.",
    codeRef: "src/api/main.py -> broadcast_event() & /api/monitor/events",
    samplePayload: {
      "stage": "TRANSCRIBED",
      "item_id": "EvF9mhGyMps",
      "progress": 60,
      "message": "Faster-Whisper CUDA speech transcription complete"
    }
  },
  "safe_deletion": {
    name: "Selective Data Management & Controlled Purge Hub",
    category: "Governance",
    type: "DATA LIFECYCLE",
    color: "#ef4444",
    description: "Provides controlled, reversible data management. Generates 2-step impact previews and requires cryptographic confirmation token 'CONFIRM_DELETE' before purging.",
    inputs: "Target scope (Channel, Date Range, Single Date), confirmation token",
    outputs: "Preview report of exact files to be deleted, deletion execution summary",
    filesProduced: "logs/data_management/audit.jsonl",
    timingTrackers: "preview_calc_sec, deletion_execution_sec",
    failureModes: "Invalid confirmation token, preview expired (>15 min), concurrent deletion conflict.",
    retryPolicy: "Strict zero-tolerance validation; aborts if token does not match CONFIRM_DELETE.",
    codeRef: "src/storage/deletion_service.py -> DeletionService.execute_deletion()",
    samplePayload: {
      "preview_id": "del_preview_9210e",
      "scope": "CHANNEL",
      "items_to_delete": 15,
      "files_to_delete": 60,
      "audit_token_verified": true
    }
  }
};

// Pipeline Step Sequence for Timeline / Mobile Card View
const PIPELINE_SEQUENCE = [
  "watchlist",
  "discovery",
  "date_filter",
  "dedup",
  "download",
  "audio_norm",
  "post_text",
  "live_segmenter",
  "transcription",
  "llm_analysis",
  "evidence_validation",
  "citation_generation",
  "summary_synthesis",
  "reports",
  "timing_profiler",
  "analytics_aggregator",
  "sse_telemetry",
  "safe_deletion"
];

// State Management
let currentTab = "full";
let currentViewMode = "diagram"; // 'diagram' | 'timeline'
let zoomScale = 1.0;
let panX = 0;
let panY = 0;
let isDragging = false;
let startX, startY;
let initialDistance = 0;

// =============================================================================
// Initialization
// =============================================================================
document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initControls();
  initViewModeToggle();
  initSearch();
  initDrawer();
  initTouchGestures();

  // Smart responsive view mode: Step-by-Step Cards for phones, Interactive Diagram for desktop
  if (window.innerWidth < 768) {
    setViewMode("timeline");
  } else {
    setViewMode("diagram");
  }
});

// =============================================================================
// View Mode Toggle (Diagram vs Detailed Step Cards)
// =============================================================================
function initViewModeToggle() {
  const btn = document.getElementById("btn-view-mode");
  if (!btn) return;

  btn.addEventListener("click", () => {
    if (currentViewMode === "diagram") {
      setViewMode("timeline");
    } else {
      setViewMode("diagram");
    }
  });
}

function setViewMode(mode) {
  currentViewMode = mode;
  const canvas = document.getElementById("diagram-canvas");
  const timeline = document.getElementById("timeline-container");
  const btn = document.getElementById("btn-view-mode");
  const controls = document.getElementById("controls-overlay");
  const legend = document.getElementById("legend-overlay");

  if (mode === "timeline") {
    canvas.style.display = "none";
    timeline.style.display = "block";
    controls.style.display = "none";
    if (legend) legend.style.display = "none";
    btn.innerHTML = '<span class="icon">🗺️</span> <span class="text">Interactive Diagram</span>';
    renderTimelineView();
  } else {
    canvas.style.display = "flex";
    timeline.style.display = "none";
    controls.style.display = "flex";
    if (legend && window.innerWidth > 992) legend.style.display = "block";
    btn.innerHTML = '<span class="icon">📋</span> <span class="text">Step-by-Step Flow</span>';
    renderDiagramView();
  }
}

// =============================================================================
// Tab Switching
// =============================================================================
function initTabs() {
  const tabs = document.querySelectorAll(".tab-btn");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      currentTab = tab.getAttribute("data-tab");
      resetTransform();
      renderCurrentView();

      // Scroll active tab into view on mobile
      tab.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
    });
  });
}

function renderCurrentView() {
  if (currentViewMode === "timeline") {
    renderTimelineView();
  } else {
    renderDiagramView();
  }
}

// =============================================================================
// Step-by-Step Timeline Mode Rendering (Mobile & Desktop Friendly)
// =============================================================================
function renderTimelineView() {
  const container = document.getElementById("timeline-container");
  if (!container) return;

  let html = `
    <div style="margin-bottom: 20px;">
      <h2 style="font-size: 18px; font-weight: 800; color: #fff; margin-bottom: 4px;">Detailed Pipeline Architecture & Sequence</h2>
      <p style="font-size: 13px; color: var(--text-secondary);">End-to-end data lifecycle stages from discovery to LLM verification and safe storage. Tap any stage to inspect complete technical specifications.</p>
    </div>
    <div class="timeline-flow">
  `;

  PIPELINE_SEQUENCE.forEach((stageId, idx) => {
    const spec = STAGE_SPECS[stageId];
    if (!spec) return;

    html += `
      <div class="timeline-card" data-stage="${stageId}">
        <div class="timeline-node-icon" style="border-color: ${spec.color};">${idx + 1}</div>
        <div class="timeline-card-header">
          <div class="timeline-card-title">${spec.name}</div>
          <span class="timeline-badge" style="background: ${spec.color}25; color: ${spec.color}; border: 1px solid ${spec.color}50;">
            ${spec.type}
          </span>
        </div>
        <div class="timeline-card-desc">${spec.description}</div>
        
        <div class="timeline-meta-grid">
          <div class="timeline-meta-item">
            <strong>Input Source</strong>
            <span>${spec.inputs}</span>
          </div>
          <div class="timeline-meta-item">
            <strong>Deliverables</strong>
            <span>${spec.outputs}</span>
          </div>
          <div class="timeline-meta-item">
            <strong>Hardware / Runtime</strong>
            <span>${spec.timingTrackers}</span>
          </div>
          <div class="timeline-meta-item">
            <strong>Code Implementation</strong>
            <span style="font-family: monospace; color: #38bdf8;">${spec.codeRef}</span>
          </div>
        </div>

        <div class="timeline-inspect-hint">
          <span>Tap to view JSON payload & failure recovery &rarr;</span>
        </div>
      </div>
    `;
  });

  html += `</div>`;
  container.innerHTML = html;

  // Add click listeners to timeline cards
  container.querySelectorAll(".timeline-card").forEach(card => {
    card.addEventListener("click", () => {
      const stage = card.getAttribute("data-stage");
      openDrawer(stage);
    });
  });
}

// =============================================================================
// Diagram View Rendering (Dynamic SVG with Responsive Canvas)
// =============================================================================
function renderDiagramView() {
  const canvas = document.getElementById("diagram-canvas");
  if (!canvas) return;

  if (currentTab === "full") {
    canvas.innerHTML = renderFullPipelineSVG();
  } else if (currentTab === "flows") {
    canvas.innerHTML = renderActivityFlowsSVG();
  } else if (currentTab === "evidence") {
    canvas.innerHTML = renderEvidenceGroundingSVG();
  } else if (currentTab === "profiling") {
    canvas.innerHTML = renderTimingProfilerSVG();
  } else if (currentTab === "storage") {
    canvas.innerHTML = renderStorageHierarchySVG();
  } else if (currentTab === "deletion") {
    canvas.innerHTML = renderDeletionAuditSVG();
  }

  // Attach click listeners to all nodes
  document.querySelectorAll(".node-group").forEach(el => {
    el.addEventListener("click", () => {
      const stage = el.getAttribute("data-stage");
      openDrawer(stage);
    });
  });

  applyTransform();
}

// =============================================================================
// SVG Helper Functions
// =============================================================================
function svgNode(x, y, w, h, stageKey, title, subtitle, color, badgeText) {
  return `
    <g class="node-group" data-stage="${stageKey}" transform="translate(${x}, ${y})">
      <rect class="node-box" width="${w}" height="${h}" />
      <line x1="0" y1="0" x2="0" y2="${h}" stroke="${color}" stroke-width="4" stroke-linecap="round" />
      <text class="node-title" x="14" y="24">${title}</text>
      <text class="node-subtitle" x="14" y="42">${subtitle}</text>
      ${badgeText ? `
        <rect x="${w - 74}" y="10" width="64" height="18" rx="4" fill="${color}20" stroke="${color}60" stroke-width="1" />
        <text class="node-badge" x="${w - 42}" y="22" fill="${color}" text-anchor="middle">${badgeText}</text>
      ` : ""}
    </g>
  `;
}

function svgEdge(x1, y1, x2, y2, markerId = "arrow", color = "#64748b", active = false) {
  return `
    <path d="M ${x1} ${y1} L ${x2} ${y2}" class="edge-path ${active ? 'flow-active' : ''}" stroke="${color}" marker-end="url(#${markerId})" />
  `;
}

function svgCurvedEdge(x1, y1, x2, y2, markerId = "arrow", color = "#64748b", active = false) {
  const dx = (x2 - x1) / 2;
  return `
    <path d="M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}" class="edge-path ${active ? 'flow-active' : ''}" stroke="${color}" marker-end="url(#${markerId})" />
  `;
}

// =============================================================================
// Diagram 1: Complete End-to-End Pipeline Swimlanes
// =============================================================================
function renderFullPipelineSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1480 840" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
      <marker id="arrow-blue" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#3b82f6" />
      </marker>
      <marker id="arrow-pink" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#ec4899" />
      </marker>
      <marker id="arrow-purple" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#8b5cf6" />
      </marker>
      <marker id="arrow-red" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#ef4444" />
      </marker>
      <marker id="arrow-green" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#10b981" />
      </marker>
    </defs>

    <!-- Background Swimlane Guides -->
    <rect x="20" y="30" width="1440" height="230" rx="14" fill="#131b2e" stroke="#243252" stroke-width="1" />
    <text x="40" y="55" fill="#64748b" font-size="11" font-weight="700" letter-spacing="1">PHASE 1: DISCOVERY, DATE FILTERING & MULTI-FLOW INGESTION</text>

    <rect x="20" y="280" width="1440" height="260" rx="14" fill="#131b2e" stroke="#243252" stroke-width="1" />
    <text x="40" y="305" fill="#64748b" font-size="11" font-weight="700" letter-spacing="1">PHASE 2: CUDA SPEECH-TO-TEXT, DUAL-ENGINE LLM & EVIDENCE GROUNDING</text>

    <rect x="20" y="560" width="1440" height="250" rx="14" fill="#131b2e" stroke="#243252" stroke-width="1" />
    <text x="40" y="585" fill="#64748b" font-size="11" font-weight="700" letter-spacing="1">PHASE 3: SYNTHESIS, MULTI-FORMAT REPORTING & SAFE DATA LIFECYCLE</text>

    <!-- Row 1 Nodes: Ingestion -->
    ${svgNode(40, 80, 190, 60, "watchlist", "Watchlist Config", "channels.yaml", "#3b82f6", "GLOBAL")}
    ${svgEdge(230, 110, 270, 110, "arrow")}

    ${svgNode(270, 80, 210, 60, "discovery", "Activity Discovery", "yt-dlp multi-tab scanner", "#3b82f6", "STAGE 1")}
    ${svgEdge(480, 110, 520, 110, "arrow")}

    ${svgNode(520, 80, 190, 60, "date_filter", "Date Filtering", "UTC window match", "#06b6d4", "FILTER")}
    ${svgEdge(710, 110, 750, 110, "arrow")}

    ${svgNode(750, 80, 200, 60, "dedup", "Deduplication Gate", "SHA-256 state check", "#8b5cf6", "GATE")}

    <!-- 4 Sub-Flow Ingestion Branches -->
    ${svgCurvedEdge(950, 110, 1020, 60, "arrow-blue", "#3b82f6", true)}
    ${svgNode(1020, 45, 190, 45, "download", "Video Ingestion", "Opus audio stream", "#3b82f6", "VIDEO")}

    ${svgCurvedEdge(950, 110, 1020, 110, "arrow-pink", "#ec4899", true)}
    ${svgNode(1020, 95, 190, 45, "download", "Shorts Ingestion", "Vertical audio stream", "#ec4899", "SHORT")}

    ${svgCurvedEdge(950, 110, 1020, 160, "arrow-purple", "#8b5cf6", true)}
    ${svgNode(1020, 145, 190, 45, "post_text", "Post Text Parser", "Direct text & images", "#8b5cf6", "POST")}

    ${svgCurvedEdge(950, 110, 1020, 210, "arrow-red", "#ef4444", true)}
    ${svgNode(1020, 195, 190, 45, "live_segmenter", "Live Stream Segmenter", "VOD catch-up capture", "#ef4444", "LIVE")}

    <!-- Media normalization & transcription -->
    ${svgEdge(1210, 68, 1260, 110, "arrow")}
    ${svgEdge(1210, 118, 1260, 110, "arrow")}
    ${svgEdge(1210, 218, 1260, 110, "arrow")}
    ${svgNode(1260, 80, 180, 60, "audio_norm", "FFmpeg Resample", "16kHz Mono WAV", "#ec4899", "AUDIO")}

    <!-- Audio -> Speech-to-text (transcription) -->
    ${svgCurvedEdge(1350, 140, 1350, 330, "arrow-green", "#10b981", true)}
    ${svgNode(1240, 330, 200, 65, "transcription", "Faster-Whisper CUDA", "float16 + Silero VAD", "#10b981", "SPEECH")}

    <!-- Post Text bypass straight to LLM -->
    ${svgCurvedEdge(1210, 168, 1040, 360, "arrow-purple", "#8b5cf6")}

    <!-- Transcription -> LLM -->
    ${svgEdge(1240, 362, 1160, 362, "arrow-green")}

    <!-- Row 2: AI & Evidence Verification -->
    ${svgNode(920, 330, 240, 65, "llm_analysis", "Dual-Engine Ollama", "gemma3:12b / qwen3:8b", "#f59e0b", "ANALYSIS")}
    ${svgEdge(920, 362, 840, 362, "arrow")}

    ${svgNode(600, 330, 240, 65, "evidence_validation", "Evidence Grounding", "Levenshtein Fuzzy >= 0.75", "#10b981", "VERIFY")}
    ${svgEdge(600, 362, 520, 362, "arrow")}

    ${svgNode(280, 330, 240, 65, "citation_generation", "Deep Citations", "[MM:SS] Video Anchors", "#06b6d4", "CITATIONS")}
    ${svgEdge(280, 362, 220, 362, "arrow")}

    ${svgNode(40, 330, 180, 65, "summary_synthesis", "Grounded Summary", "Executive Takeaways", "#3b82f6", "SYNTHESIS")}

    <!-- Connect Row 2 to Row 3 -->
    ${svgCurvedEdge(130, 395, 130, 610, "arrow")}

    <!-- Row 3: Reports, Telemetry, and Safe Governance -->
    ${svgNode(40, 610, 210, 65, "reports", "File-First Reports", "HTML + JSON + Markdown", "#8b5cf6", "REPORTS")}
    ${svgEdge(250, 642, 310, 642, "arrow")}

    ${svgNode(310, 610, 230, 65, "timing_profiler", "Timing Profiler", "Microsecond Profiling", "#f59e0b", "PROFILER")}
    ${svgEdge(540, 642, 600, 642, "arrow")}

    ${svgNode(600, 610, 240, 65, "analytics_aggregator", "Analytics Aggregator", "Date-Range Intelligence", "#06b6d4", "METRICS")}
    ${svgEdge(840, 642, 900, 642, "arrow")}

    ${svgNode(900, 610, 230, 65, "sse_telemetry", "SSE Live Telemetry", "/api/monitor/events", "#10b981", "STREAMING")}
    ${svgEdge(1130, 642, 1190, 642, "arrow")}

    ${svgNode(1190, 610, 250, 65, "safe_deletion", "Safe Deletion Hub", "2-Step Confirmation & Audit", "#ef4444", "GOVERN")}
  </svg>
  `;
}

// =============================================================================
// Diagram 2: 4 Activity Flows Dedicated Swimlanes
// =============================================================================
function renderActivityFlowsSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1420 800" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
    </defs>

    <!-- Lane 1: Long-Form Videos -->
    <rect x="20" y="30" width="1380" height="160" rx="12" fill="#131b2e" stroke="#3b82f6" stroke-width="1.5" stroke-dasharray="8 4" />
    <text x="40" y="58" fill="#3b82f6" font-size="12" font-weight="800">FLOW 1: LONG-FORM VIDEOS (Multi-minute speeches, debates, press conferences)</text>
    ${svgNode(40, 75, 200, 55, "discovery", "yt-dlp /videos Tab", "Extract videoId & metadata", "#3b82f6", "DISCOVERY")}
    ${svgEdge(240, 102, 290, 102, "arrow")}
    ${svgNode(290, 75, 210, 55, "download", "Opus Audio Stream", "Best quality opus / m4a", "#3b82f6", "AUDIO")}
    ${svgEdge(500, 102, 550, 102, "arrow")}
    ${svgNode(550, 75, 230, 55, "transcription", "Whisper CUDA Speech", "Float16 multilingual VAD", "#10b981", "TRANSCRIPT")}
    ${svgEdge(780, 102, 830, 102, "arrow")}
    ${svgNode(830, 75, 240, 55, "llm_analysis", "Dual-Engine LLM", "Gemma 3 12B / Qwen3 8B", "#f59e0b", "ANALYSIS")}
    ${svgEdge(1070, 102, 1120, 102, "arrow")}
    ${svgNode(1120, 75, 260, 55, "evidence_validation", "Timestamp Grounding", "Verified claims with [MM:SS]", "#06b6d4", "VERIFIED")}

    <!-- Lane 2: YouTube Shorts -->
    <rect x="20" y="215" width="1380" height="160" rx="12" fill="#131b2e" stroke="#ec4899" stroke-width="1.5" stroke-dasharray="8 4" />
    <text x="40" y="243" fill="#ec4899" font-size="12" font-weight="800">FLOW 2: YOUTUBE SHORTS (<= 60s vertical clips, soundbites, high-energy updates)</text>
    ${svgNode(40, 260, 200, 55, "discovery", "yt-dlp /shorts Tab", "Extract shortId & caption", "#ec4899", "DISCOVERY")}
    ${svgEdge(240, 287, 290, 287, "arrow")}
    ${svgNode(290, 260, 210, 55, "download", "Fast Audio Extraction", "Short stream download", "#ec4899", "AUDIO")}
    ${svgEdge(500, 287, 550, 287, "arrow")}
    ${svgNode(550, 260, 230, 55, "transcription", "Whisper Word Timestamps", "High temporal precision", "#10b981", "TRANSCRIPT")}
    ${svgEdge(780, 287, 830, 287, "arrow")}
    ${svgNode(830, 260, 240, 55, "llm_analysis", "Short-Form LLM Prompt", "Soundbite & claim triage", "#f59e0b", "ANALYSIS")}
    ${svgEdge(1070, 287, 1120, 287, "arrow")}
    ${svgNode(1120, 260, 260, 55, "citation_generation", "Shorts Citation Links", "Direct short anchor URLs", "#06b6d4", "VERIFIED")}

    <!-- Lane 3: Community Posts -->
    <rect x="20" y="400" width="1380" height="160" rx="12" fill="#131b2e" stroke="#8b5cf6" stroke-width="1.5" stroke-dasharray="8 4" />
    <text x="40" y="428" fill="#8b5cf6" font-size="12" font-weight="800">FLOW 3: COMMUNITY POSTS (Audio-free text, announcements, posters, photo carousels)</text>
    ${svgNode(40, 445, 200, 55, "discovery", "InnerTube /community", "Extract post text & images", "#8b5cf6", "DISCOVERY")}
    ${svgEdge(240, 472, 330, 472, "arrow")}
    ${svgNode(330, 445, 240, 55, "post_text", "Direct Text Parser", "Zero audio bypass", "#8b5cf6", "TEXT")}
    ${svgEdge(570, 472, 830, 472, "arrow")}
    ${svgNode(830, 445, 240, 55, "llm_analysis", "Post Analysis Prompt", "Policy & announcement extraction", "#f59e0b", "ANALYSIS")}
    ${svgEdge(1070, 472, 1120, 472, "arrow")}
    ${svgNode(1120, 445, 260, 55, "citation_generation", "Post Permalinks", "Direct /post/Ugkx... citations", "#06b6d4", "VERIFIED")}

    <!-- Lane 4: YouTube Live -->
    <rect x="20" y="585" width="1380" height="160" rx="12" fill="#131b2e" stroke="#ef4444" stroke-width="1.5" stroke-dasharray="8 4" />
    <text x="40" y="613" fill="#ef4444" font-size="12" font-weight="800">FLOW 4: YOUTUBE LIVE (Real-time parliamentary coverage, state events, pressers)</text>
    ${svgNode(40, 630, 200, 55, "discovery", "yt-dlp /streams Tab", "Status: active / was_live", "#ef4444", "DISCOVERY")}
    ${svgEdge(240, 657, 290, 657, "arrow")}
    ${svgNode(290, 630, 210, 55, "live_segmenter", "Stream Buffer Collector", "Chunked segment ingest", "#ef4444", "STREAM")}
    ${svgEdge(500, 657, 550, 657, "arrow")}
    ${svgNode(550, 630, 230, 55, "transcription", "Catch-up Whisper CUDA", "Sliding window transcription", "#10b981", "TRANSCRIPT")}
    ${svgEdge(780, 657, 830, 657, "arrow")}
    ${svgNode(830, 630, 240, 55, "llm_analysis", "Dual-Engine Ollama", "Gemma 3 12B / Qwen3 8B", "#f59e0b", "ANALYSIS")}
    ${svgEdge(1070, 657, 1120, 657, "arrow")}
    ${svgNode(1120, 630, 260, 55, "evidence_validation", "Live Stream Citations", "Exact broadcast timestamp anchors", "#06b6d4", "VERIFIED")}
  </svg>
  `;
}

// =============================================================================
// Diagram 3: Grounded Citations Flow
// =============================================================================
function renderEvidenceGroundingSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1320 680" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
    </defs>

    ${svgNode(40, 100, 260, 75, "transcription", "Raw Whisper Transcript", "Timestamped word segments", "#10b981", "GROUND TRUTH")}
    ${svgEdge(300, 137, 370, 137, "arrow")}

    ${svgNode(370, 100, 270, 75, "llm_analysis", "Candidate Claims (LLM)", "Unverified assertions & quotes", "#f59e0b", "CANDIDATES")}
    ${svgEdge(640, 137, 710, 137, "arrow")}

    ${svgNode(710, 100, 280, 75, "evidence_validation", "Levenshtein Fuzzy Matcher", "Threshold ratio >= 0.75", "#10b981", "ALGORITHM")}

    <!-- Split into Validated vs Hallucination Rejection -->
    ${svgCurvedEdge(990, 137, 1070, 75, "arrow")}
    ${svgNode(1070, 45, 210, 60, "citation_generation", "Verified Grounded Claim", "Anchor to [MM:SS]", "#06b6d4", "PASSED")}

    ${svgCurvedEdge(990, 137, 1070, 185, "arrow")}
    ${svgNode(1070, 160, 210, 60, "evidence_validation", "Hallucination Discarded", "Filtered from final report", "#ef4444", "REJECTED")}

    <!-- Provenance Synthesis -->
    ${svgCurvedEdge(1175, 105, 1175, 340, "arrow")}
    ${svgNode(1030, 340, 250, 75, "summary_synthesis", "Grounded Executive Summary", "Interactive [1], [2] badges", "#3b82f6", "SYNTHESIS")}
    ${svgEdge(1030, 377, 920, 377, "arrow")}

    ${svgNode(650, 340, 270, 75, "citation_generation", "Second-Level Video URLs", "?t=XXs clickable links", "#06b6d4", "AUDITABLE")}
    ${svgEdge(650, 377, 540, 377, "arrow")}

    ${svgNode(280, 340, 260, 75, "reports", "Interactive HTML & Markdown", "Full transparency audit trail", "#8b5cf6", "DELIVERABLE")}
  </svg>
  `;
}

// =============================================================================
// Diagram 4: Processing-Time Profiler
// =============================================================================
function renderTimingProfilerSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1320 680" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
    </defs>

    ${svgNode(40, 80, 260, 75, "download", "Download Phase", "Audio stream I/O latency", "#ec4899", "IO TIMING")}
    ${svgEdge(300, 117, 360, 117, "arrow")}

    ${svgNode(360, 80, 260, 75, "audio_norm", "FFmpeg Extraction", "16kHz resampling duration", "#ec4899", "CPU TIMING")}
    ${svgEdge(620, 117, 680, 117, "arrow")}

    ${svgNode(680, 80, 280, 75, "transcription", "CUDA Transcription", "Whisper GPU inference time", "#10b981", "GPU TIMING")}
    ${svgEdge(960, 117, 1020, 117, "arrow")}

    ${svgNode(1020, 80, 260, 75, "llm_analysis", "Ollama LLM Phase", "Generation tokens / sec", "#f59e0b", "LLM TIMING")}

    <!-- Convergence to Profiler Record -->
    ${svgCurvedEdge(1150, 155, 1150, 300, "arrow")}
    ${svgNode(1000, 300, 280, 80, "timing_profiler", "ActivityTiming Record", "Microsecond perf_counter()", "#f59e0b", "PERSISTENCE")}
    ${svgEdge(1000, 340, 880, 340, "arrow")}

    ${svgNode(600, 300, 280, 80, "analytics_aggregator", "Dashboard Aggregator", "P95, Average & Bottlenecks", "#06b6d4", "ANALYTICS")}
    ${svgEdge(600, 340, 480, 340, "arrow")}

    ${svgNode(200, 300, 280, 80, "sse_telemetry", "Real-Time Telemetry Bar", "Live UI seconds display", "#10b981", "UI DISPLAY")}
  </svg>
  `;
}

// =============================================================================
// Diagram 5: File-First Storage Tree
// =============================================================================
function renderStorageHierarchySVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1320 720" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
    </defs>

    ${svgNode(40, 50, 260, 60, "watchlist", "data/channels/", "Channel configs & history", "#3b82f6", "DIR")}
    ${svgNode(40, 140, 260, 60, "discovery", "data/activities/YYYY/MM/DD/", "videos, shorts, posts, live", "#3b82f6", "DIR")}
    ${svgNode(40, 230, 260, 60, "transcription", "data/transcripts/", "{item_id}.json transcripts", "#10b981", "DIR")}
    ${svgNode(40, 320, 260, 60, "llm_analysis", "data/analysis/", "{item_id}.json grounded AI", "#f59e0b", "DIR")}
    ${svgNode(40, 410, 260, 60, "reports", "data/reports/daily/", "{YYYY-MM-DD}.html / json / md", "#8b5cf6", "DIR")}
    ${svgNode(40, 500, 260, 60, "reports", "data/reports/items/", "{item_id}.html / json standalone", "#8b5cf6", "DIR")}
    ${svgNode(40, 590, 260, 60, "timing_profiler", "data/analytics/timing/", "Microsecond latency JSONs", "#06b6d4", "DIR")}

    <!-- State & Audit Columns -->
    ${svgNode(460, 140, 280, 75, "dedup", "state/items/{item_id}.json", "Idempotency & stage checkpoints", "#8b5cf6", "STATE")}
    ${svgNode(460, 320, 280, 75, "safe_deletion", "logs/data_management/", "audit.jsonl tamper-proof log", "#ef4444", "AUDIT")}
    ${svgNode(460, 500, 280, 75, "sse_telemetry", "logs/runs/{run_id}/", "events.jsonl runtime stream", "#10b981", "LOGS")}

    <!-- Architecture Pillars -->
    ${svgNode(880, 200, 380, 120, "reports", "File-First Core Guarantees", "Zero SQL • Predictable NVMe Disk Layout • Git-Friendly • 100% Local Auditability", "#38bdf8", "PILLAR 1")}
    ${svgNode(880, 380, 380, 120, "safe_deletion", "Safe Deletion Governance", "Every file deletion leaves an immutable record in audit.jsonl with exact byte count and SHA-256 tokens.", "#ef4444", "PILLAR 2")}
  </svg>
  `;
}

// =============================================================================
// Diagram 6: Safe Deletion & Audit Lifecycle
// =============================================================================
function renderDeletionAuditSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1320 680" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#64748b" />
      </marker>
    </defs>

    ${svgNode(40, 100, 260, 75, "safe_deletion", "Deletion Request", "Target channel or date range", "#ef4444", "STEP 1")}
    ${svgEdge(300, 137, 360, 137, "arrow")}

    ${svgNode(360, 100, 280, 75, "safe_deletion", "Impact Preview Calculation", "Exact files, items & bytes counted", "#f59e0b", "STEP 2")}
    ${svgEdge(640, 137, 700, 137, "arrow")}

    ${svgNode(700, 100, 280, 75, "safe_deletion", "User Review & Token Entry", "Requires 'CONFIRM_DELETE'", "#ef4444", "GATE")}

    <!-- Branch on Token Match -->
    ${svgCurvedEdge(980, 137, 1060, 75, "arrow")}
    ${svgNode(1060, 45, 220, 65, "safe_deletion", "Atomic File Deletion", "Removes activities, transcripts", "#10b981", "EXECUTED")}

    ${svgCurvedEdge(980, 137, 1060, 185, "arrow")}
    ${svgNode(1060, 160, 220, 65, "safe_deletion", "Purge Aborted (400)", "No files touched", "#ef4444", "ABORTED")}

    <!-- Audit log output -->
    ${svgCurvedEdge(1170, 110, 1170, 320, "arrow")}
    ${svgNode(1020, 320, 260, 75, "safe_deletion", "Immutable Audit Record", "Appends to audit.jsonl", "#3b82f6", "AUDIT")}
    ${svgEdge(1020, 357, 880, 357, "arrow")}

    ${svgNode(580, 320, 300, 75, "analytics_aggregator", "Dashboard Cache Invalidation", "Refreshes live metrics & charts", "#06b6d4", "CACHE")}
  </svg>
  `;
}

// =============================================================================
// Slide-Over / Bottom-Sheet Stage Detail Drawer
// =============================================================================
function openDrawer(stageKey) {
  const spec = STAGE_SPECS[stageKey];
  if (!spec) return;

  const drawer = document.getElementById("detail-drawer");
  const backdrop = document.getElementById("drawer-backdrop");

  document.getElementById("drawer-stage-badge").textContent = spec.type || "STAGE";
  document.getElementById("drawer-stage-badge").style.color = spec.color || "#38bdf8";
  document.getElementById("drawer-stage-title").textContent = spec.name;
  document.getElementById("drawer-desc").textContent = spec.description;
  document.getElementById("drawer-input").textContent = spec.inputs;
  document.getElementById("drawer-output").textContent = spec.outputs;

  // Files List
  const filesList = document.getElementById("drawer-files-list");
  filesList.innerHTML = "";
  if (spec.filesProduced) {
    spec.filesProduced.split(",").forEach(f => {
      const span = document.createElement("span");
      span.className = "tag";
      span.textContent = f.trim();
      filesList.appendChild(span);
    });
  }

  document.getElementById("drawer-timing").textContent = spec.timingTrackers;
  document.getElementById("drawer-failure").textContent = spec.failureModes;
  document.getElementById("drawer-retry").textContent = spec.retryPolicy;
  document.getElementById("drawer-code").textContent = spec.codeRef;
  document.getElementById("drawer-payload").textContent = JSON.stringify(spec.samplePayload, null, 2);

  drawer.classList.add("open");
  if (backdrop) backdrop.classList.add("active");
}

function closeDrawer() {
  const drawer = document.getElementById("detail-drawer");
  const backdrop = document.getElementById("drawer-backdrop");
  if (drawer) drawer.classList.remove("open");
  if (backdrop) backdrop.classList.remove("active");
}

function initDrawer() {
  const closeBtn = document.getElementById("drawer-close");
  const backdrop = document.getElementById("drawer-backdrop");

  if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
  if (backdrop) backdrop.addEventListener("click", closeDrawer);

  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeDrawer();
  });
}

// =============================================================================
// Touch & Pan / Zoom Controls
// =============================================================================
function initControls() {
  const btnIn = document.getElementById("btn-zoom-in");
  const btnOut = document.getElementById("btn-zoom-out");
  const btnReset = document.getElementById("btn-reset");
  const btnFit = document.getElementById("btn-fit");

  if (btnIn) btnIn.addEventListener("click", () => zoom(1.2));
  if (btnOut) btnOut.addEventListener("click", () => zoom(0.8));
  if (btnReset) btnReset.addEventListener("click", resetTransform);
  if (btnFit) btnFit.addEventListener("click", fitToScreen);

  // Mouse wheel zoom
  const viewport = document.getElementById("viewport-container");
  if (viewport) {
    viewport.addEventListener("wheel", e => {
      if (currentViewMode === "timeline") return;
      e.preventDefault();
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      zoom(delta);
    }, { passive: false });

    // Mouse drag pan
    viewport.addEventListener("mousedown", e => {
      if (currentViewMode === "timeline") return;
      if (e.target.closest(".controls-overlay") || e.target.closest(".detail-drawer") || e.target.closest(".node-group")) return;
      isDragging = true;
      startX = e.clientX - panX;
      startY = e.clientY - panY;
    });

    window.addEventListener("mousemove", e => {
      if (!isDragging) return;
      panX = e.clientX - startX;
      panY = e.clientY - startY;
      applyTransform();
    });

    window.addEventListener("mouseup", () => {
      isDragging = false;
    });
  }
}

function initTouchGestures() {
  const viewport = document.getElementById("viewport-container");
  if (!viewport) return;

  viewport.addEventListener("touchstart", e => {
    if (currentViewMode === "timeline") return;
    if (e.touches.length === 1) {
      isDragging = true;
      startX = e.touches[0].clientX - panX;
      startY = e.touches[0].clientY - panY;
    } else if (e.touches.length === 2) {
      isDragging = false;
      initialDistance = Math.hypot(
        e.touches[0].clientX - e.touches[1].clientX,
        e.touches[0].clientY - e.touches[1].clientY
      );
    }
  }, { passive: true });

  viewport.addEventListener("touchmove", e => {
    if (currentViewMode === "timeline") return;
    if (e.touches.length === 1 && isDragging) {
      panX = e.touches[0].clientX - startX;
      panY = e.touches[0].clientY - startY;
      applyTransform();
    } else if (e.touches.length === 2) {
      const currentDistance = Math.hypot(
        e.touches[0].clientX - e.touches[1].clientX,
        e.touches[0].clientY - e.touches[1].clientY
      );
      if (initialDistance > 0) {
        const factor = currentDistance / initialDistance;
        zoom(factor);
        initialDistance = currentDistance;
      }
    }
  }, { passive: true });

  viewport.addEventListener("touchend", () => {
    isDragging = false;
    initialDistance = 0;
  });
}

function zoom(factor) {
  zoomScale *= factor;
  zoomScale = Math.max(0.4, Math.min(3.0, zoomScale));
  applyTransform();
}

function resetTransform() {
  zoomScale = window.innerWidth < 768 ? 0.85 : 1.0;
  panX = 0;
  panY = 0;
  applyTransform();
}

function fitToScreen() {
  const canvas = document.getElementById("diagram-canvas");
  if (!canvas) return;
  const svg = canvas.querySelector("svg");
  if (!svg) return;

  const rect = canvas.getBoundingClientRect();
  const vb = svg.viewBox.baseVal;
  if (vb && vb.width > 0 && vb.height > 0) {
    const scaleX = rect.width / vb.width;
    const scaleY = rect.height / vb.height;
    zoomScale = Math.min(scaleX, scaleY) * 0.95;
    panX = 0;
    panY = 0;
    applyTransform();
  }
}

function applyTransform() {
  const canvas = document.getElementById("diagram-canvas");
  if (canvas) {
    const svg = canvas.querySelector("svg");
    if (svg) {
      svg.style.transform = `translate(${panX}px, ${panY}px) scale(${zoomScale})`;
      svg.style.transformOrigin = "center center";
      svg.style.transition = isDragging ? "none" : "transform 0.15s ease-out";
    }
  }
}

// =============================================================================
// Search Functionality (Filters both SVG Nodes and Timeline Cards)
// =============================================================================
function initSearch() {
  const input = document.getElementById("search-input");
  if (!input) return;

  input.addEventListener("input", e => {
    const term = e.target.value.toLowerCase().trim();

    // 1. Highlight in SVG Canvas
    document.querySelectorAll(".node-group").forEach(group => {
      const stageKey = group.getAttribute("data-stage");
      const spec = STAGE_SPECS[stageKey];
      const box = group.querySelector(".node-box");

      if (!term) {
        box.classList.remove("highlight");
        group.style.opacity = "1";
        return;
      }

      if (spec && (
        spec.name.toLowerCase().includes(term) ||
        spec.description.toLowerCase().includes(term) ||
        spec.inputs.toLowerCase().includes(term) ||
        spec.outputs.toLowerCase().includes(term) ||
        spec.filesProduced.toLowerCase().includes(term)
      )) {
        box.classList.add("highlight");
        group.style.opacity = "1";
      } else {
        box.classList.remove("highlight");
        group.style.opacity = "0.2";
      }
    });

    // 2. Filter in Timeline Mode
    document.querySelectorAll(".timeline-card").forEach(card => {
      const stageKey = card.getAttribute("data-stage");
      const spec = STAGE_SPECS[stageKey];

      if (!term) {
        card.style.display = "block";
        return;
      }

      if (spec && (
        spec.name.toLowerCase().includes(term) ||
        spec.description.toLowerCase().includes(term) ||
        spec.inputs.toLowerCase().includes(term) ||
        spec.outputs.toLowerCase().includes(term) ||
        spec.filesProduced.toLowerCase().includes(term)
      )) {
        card.style.display = "block";
      } else {
        card.style.display = "none";
      }
    });
  });
}
