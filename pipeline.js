/**
 * Standalone Pipeline Flow Interactive Documentation System
 * Zero external dependencies - pure vanilla JS & SVG.
 */

const STAGE_SPECS = {
  // Discovery & Ingestion
  "watchlist": {
    name: "Channel Watchlist",
    category: "Ingestion",
    type: "GLOBAL",
    color: "#3b82f6",
    description: "Monitored channels loaded from YAML configuration file. Configures channel ID, handle, name, and tracking parameters.",
    inputs: "config/channels.yaml, config/settings.yaml",
    outputs: "ChannelConfig objects in memory",
    filesProduced: "config/channels.yaml",
    timingTrackers: "N/A (Configuration load)",
    failureModes: "YAML parsing error, missing required fields (id, name)",
    retryPolicy: "Immediate abort on startup with error log",
    codeRef: "src/config.py -> Settings.load_channels()",
    samplePayload: {
      "id": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
      "name": "Google Developers",
      "custom_url": "@GoogleDevelopers",
      "active": true
    }
  },
  "discovery": {
    name: "Activity Discovery",
    category: "Discovery",
    type: "ALL",
    color: "#3b82f6",
    description: "Queries YouTube via yt-dlp extracting recent tab entries: Videos, Shorts, Community Posts, and active/upcoming Live streams.",
    inputs: "Target date, date range, or current timestamp, channel URL",
    outputs: "Raw activity metadata entries",
    filesProduced: "data/activities/YYYY/MM/DD/{type}/{item_id}.json",
    timingTrackers: "discovery_start, discovery_end, stage_duration_seconds",
    failureModes: "YouTube rate-limiting, network timeout, channel deleted/private",
    retryPolicy: "Exponential backoff up to 3 retries (settings.llm_max_retries)",
    codeRef: "src/collector/youtube.py -> YouTubeCollector.discover_activities()",
    samplePayload: {
      "activity_type": "VIDEO",
      "item_id": "d6He6eSQnhA",
      "channel_id": "UC_x5XG1OV2P6uZZ5FSM9Ttw",
      "title": "State Development & Infrastructure Review",
      "published_at": "2026-09-11T14:30:00Z"
    }
  },
  "dedup": {
    name: "Deduplication & State Check",
    category: "Ingestion",
    type: "ALL",
    color: "#8b5cf6",
    description: "Checks state database/files to verify if the item has already been successfully processed or if re-processing is forced.",
    inputs: "item_id, force_reprocess flag",
    outputs: "Boolean decision (process vs skip)",
    filesProduced: "state/items/{item_id}.json",
    timingTrackers: "metadata_start, metadata_end",
    failureModes: "State file corrupted or unreadable",
    retryPolicy: "Falls back to re-processing item if state unreadable",
    codeRef: "src/storage/file_store.py -> FileStore.get_item_state()",
    samplePayload: {
      "item_id": "d6He6eSQnhA",
      "stage": "COMPLETED",
      "first_seen_at": "2026-09-11T15:00:00Z",
      "last_processed_at": "2026-09-11T15:10:00Z"
    }
  },
  "download": {
    name: "Media Download & Extraction",
    category: "Processing",
    type: "VIDEO, SHORT, LIVE",
    color: "#ec4899",
    description: "Downloads high-efficiency audio stream (opus / m4a) using yt-dlp into local scratch/raw storage.",
    inputs: "YouTube item URL, format options",
    outputs: "Local audio file in raw directory",
    filesProduced: "data/raw/{item_id}/audio.opus",
    timingTrackers: "download_start, download_end, duration_seconds",
    failureModes: "Download interrupted, geoblocked content, DRM protected",
    retryPolicy: "3 retries with 5-second delays",
    codeRef: "src/collector/downloader.py -> download_audio()",
    samplePayload: {
      "item_id": "d6He6eSQnhA",
      "file_path": "data/raw/d6He6eSQnhA/audio.opus",
      "file_size_bytes": 14285700,
      "audio_codec": "opus"
    }
  },
  "audio_extraction": {
    name: "FFmpeg Audio Normalization",
    category: "Processing",
    type: "VIDEO, SHORT, LIVE",
    color: "#ec4899",
    description: "Converts raw audio stream to standard 16kHz mono 16-bit WAV for faster-whisper VAD ingestion.",
    inputs: "data/raw/{item_id}/audio.opus",
    outputs: "data/raw/{item_id}/audio_16k.wav",
    filesProduced: "data/raw/{item_id}/audio_16k.wav",
    timingTrackers: "audio_extraction_start, audio_extraction_end, duration_seconds",
    failureModes: "FFmpeg binary missing, corrupted audio stream",
    retryPolicy: "Immediate failure logged to debug/audio/{item_id}.log",
    codeRef: "src/transcription/audio_utils.py -> prepare_wav_16k()",
    samplePayload: {
      "sample_rate": 16000,
      "channels": 1,
      "format": "s16le",
      "duration": 482.4
    }
  },
  "transcription": {
    name: "Multilingual Faster-Whisper",
    category: "Transcription",
    type: "VIDEO, SHORT, LIVE",
    color: "#10b981",
    description: "Transcribes audio with word and segment timestamps, detecting Hindi, Hinglish, and English with Silero VAD filtering on CUDA GPU.",
    inputs: "data/raw/{item_id}/audio_16k.wav",
    outputs: "Timestamped Transcript JSON and text segments",
    filesProduced: "data/transcripts/{item_id}.json",
    timingTrackers: "transcription_start, transcription_end, whisper_duration_seconds",
    failureModes: "CUDA Out-of-Memory, VAD silence, corrupted audio",
    retryPolicy: "Automatic fallback to CPU float32 if CUDA OOM occurs",
    codeRef: "src/transcription/whisper_engine.py -> WhisperEngine.transcribe()",
    samplePayload: {
      "item_id": "d6He6eSQnhA",
      "language": "hi",
      "segments": [
        {
          "start": 0.0,
          "end": 4.52,
          "text": "उत्तराखंड में नए इंफ्रास्ट्रक्चर प्रोजेक्ट्स का शिलान्यास किया गया।"
        }
      ]
    }
  },
  "post_text": {
    name: "Community Post Text Extraction",
    category: "Ingestion",
    type: "POST",
    color: "#8b5cf6",
    description: "Directly normalizes community post text, attached images, poll options, and external links without requiring audio downloading or transcription.",
    inputs: "Raw YouTube Post payload",
    outputs: "Canonical Post Activity JSON with post text & URL",
    filesProduced: "data/activities/YYYY/MM/DD/posts/{item_id}.json",
    timingTrackers: "metadata_start, metadata_end",
    failureModes: "Post deleted, text unavailable",
    retryPolicy: "Logged as skipped",
    codeRef: "src/collector/youtube.py -> extract_post_data()",
    samplePayload: {
      "item_id": "UgkxVdbvYH6EoENV_0QVk3mmd1EgtzS-fNAB",
      "post_text": "आज राज्य मंत्रिपरिषद की अहम बैठक में जनकल्याणकारी निर्णयों पर मुहर लगी।",
      "url": "https://www.youtube.com/post/UgkxVdbvYH6EoENV_0QVk3mmd1EgtzS-fNAB"
    }
  },
  "chunking": {
    name: "Timestamp-Preserving Chunking",
    category: "Analysis",
    type: "LONG VIDEO",
    color: "#06b6d4",
    description: "Segments long transcripts into coherent semantic windows (e.g. 5-minute overlaps) preserving millisecond timestamps for each token.",
    inputs: "Transcript segments list",
    outputs: "List of chunk objects with timestamp boundaries",
    filesProduced: "In-memory / debug chunk cache",
    timingTrackers: "analysis_start (pre-prompting step)",
    failureModes: "Transcript empty or tokens exceed context window",
    retryPolicy: "Dynamic window resizing",
    codeRef: "src/analysis/chunker.py -> chunk_transcript()",
    samplePayload: {
      "chunk_index": 0,
      "start_time": 0.0,
      "end_time": 300.0,
      "segment_count": 48
    }
  },
  "llm_analysis": {
    name: "Ollama LLM Multilingual Analysis",
    category: "Analysis",
    type: "ALL",
    color: "#f59e0b",
    description: "Invokes primary model (gemma3:12b / qwen3:8b) extracting structured JSON: topics, entities, claims, promises, announcements, and evidence objects.",
    inputs: "Transcript / post text + System Prompt with JSON Schema",
    outputs: "Pydantic validated ItemAnalysis instance",
    filesProduced: "data/analysis/{item_id}.json, data/debug/llm/{item_id}/",
    timingTrackers: "analysis_start, analysis_end, llm_duration_seconds",
    failureModes: "LLM JSON schema invalid, connection timeout, hallucination",
    retryPolicy: "Up to 3 retries with schema re-prompting; fallback model on exhaustion",
    codeRef: "src/analysis/ollama_engine.py -> OllamaAnalyzer.analyze()",
    samplePayload: {
      "item_id": "d6He6eSQnhA",
      "topics": ["Infrastructure", "Economy", "E-Governance"],
      "claims": [
        {
          "claim": "500 crore rupee budget allocated for highway expansion.",
          "speaker": "Chief Minister",
          "evidence_text": "हमने राजमार्ग विस्तार के लिए 500 करोड़ का बजट स्वीकृत किया है।"
        }
      ]
    }
  },
  "evidence_validation": {
    name: "Evidence Grounding & Verification",
    category: "Grounding",
    type: "ALL",
    color: "#10b981",
    description: "Fuzzy string matches extracted evidence against raw transcripts or post text. Computes Levenshtein ratio and rejects ungrounded claims.",
    inputs: "ItemAnalysis claims/statements vs Transcript/Post text",
    outputs: "Verified Evidence Objects with exact timestamps / citation URLs",
    filesProduced: "Included within data/analysis/{item_id}.json",
    timingTrackers: "evidence_validation_start, evidence_validation_end",
    failureModes: "Evidence text not found in source (hallucination)",
    retryPolicy: "Rejects ungrounded statements from final report summary",
    codeRef: "src/analysis/evidence.py -> EvidenceGrounder.verify_evidence()",
    samplePayload: {
      "evidence_id": "ev_d6He6e_01",
      "verified": true,
      "match_score": 0.96,
      "timestamp_start": 42.5,
      "timestamp_end": 48.0,
      "citation_url": "https://www.youtube.com/watch?v=d6He6eSQnhA&t=42s"
    }
  },
  "citation_generation": {
    name: "Citation & Deep Linking",
    category: "Grounding",
    type: "ALL",
    color: "#06b6d4",
    description: "Constructs direct interactive citation links: timestamped URLs (&t=X) for videos/shorts/live, or permalinks for community posts.",
    inputs: "Source URL, timestamp start/end, or post ID",
    outputs: "Clickable URL strings embedded into statements",
    filesProduced: "Embedded in analysis and report JSON/HTML",
    timingTrackers: "evidence_validation_end",
    failureModes: "Malformed timestamp or URL",
    retryPolicy: "Fallback to base video/post URL",
    codeRef: "src/analysis/citation.py -> format_citation_url()",
    samplePayload: {
      "citation_url": "https://www.youtube.com/watch?v=d6He6eSQnhA&t=42s"
    }
  },
  "summary_synthesis": {
    name: "Evidence-Grounded Summary & Synthesis",
    category: "Reporting",
    type: "ALL",
    color: "#3b82f6",
    description: "Generates Executive Summary and Key Points derived STRICTLY from verified evidence items, ensuring zero hallucinated claims.",
    inputs: "Verified claims, announcements, statistics",
    outputs: "Executive Summary and Key Points with inline citation IDs",
    filesProduced: "Included in report JSON & HTML",
    timingTrackers: "report_generation_start",
    failureModes: "Empty evidence catalog",
    retryPolicy: "Displays 'No verified statements extracted' rather than fabricated text",
    codeRef: "src/reports/item_report.py -> ItemReportGenerator.generate()",
    samplePayload: {
      "executive_summary": [
        {
          "statement": "Cabinet announced 500 cr infrastructure scheme.",
          "evidence_id": "ev_d6He6e_01",
          "citation_url": "https://www.youtube.com/watch?v=d6He6eSQnhA&t=42s"
        }
      ]
    }
  },
  "reports": {
    name: "Multi-Format Report Generation",
    category: "Reporting",
    type: "ALL",
    color: "#8b5cf6",
    description: "Compiles individual item reports, channel summaries, and comprehensive cross-channel daily reports into JSON and standalone HTML files.",
    inputs: "Activity, Transcript, Analysis, Grounded Summaries",
    outputs: "Item Reports, Daily Reports, Channel Reports in JSON & HTML",
    filesProduced: "data/reports/items/{id}.html, data/reports/daily/{date}.html",
    timingTrackers: "report_generation_start, report_generation_end",
    failureModes: "Template rendering error",
    retryPolicy: "Saves JSON report and retries HTML generation with fallback template",
    codeRef: "src/reports/daily_report.py, src/reports/html_templates.py",
    samplePayload: {
      "monitoring_date": "2026-09-11",
      "summary": { "total_items": 6, "videos": 3, "shorts": 2, "posts": 1 }
    }
  },
  "timing_profiler": {
    name: "Activity Timing Profiler",
    category: "Observability",
    type: "ALL",
    color: "#f59e0b",
    description: "Captures microsecond-precision wall-clock time for each stage, tracking Whisper time, Ollama time, and saving persistent timing records.",
    inputs: "Stage entry/exit hooks throughout pipeline",
    outputs: "ActivityTiming record with StageTiming dictionary",
    filesProduced: "data/analytics/timing/{item_id}.json",
    timingTrackers: "All stage start/end stamps, wall_clock vs sum_of_stages",
    failureModes: "Filesystem write error",
    retryPolicy: "Silent fallback with warning log; pipeline execution continues",
    codeRef: "src/engine/pipeline.py, src/storage/file_store.py",
    samplePayload: {
      "activity_id": "d6He6eSQnhA",
      "wall_clock_duration_seconds": 45.2,
      "stages": {
        "transcription": { "duration_seconds": 18.4, "status": "COMPLETED" },
        "analysis": { "duration_seconds": 21.1, "status": "COMPLETED" }
      }
    }
  },
  "analytics_aggregator": {
    name: "Analytics Aggregator & Dashboard Service",
    category: "Analytics",
    type: "GLOBAL",
    color: "#06b6d4",
    description: "Computes date-range aggregations, time-series trends, content distributions, channel rankings, and stage bottlenecks dynamically from stored files.",
    inputs: "Filter parameters: date range, channels, activity types",
    outputs: "Aggregated metrics, daily buckets, percent distributions",
    filesProduced: "Dynamic API responses (zero static hardcoding)",
    timingTrackers: "Query execution timing",
    failureModes: "No files matching criteria (graceful empty states)",
    retryPolicy: "Returns structured empty payload with zero counts",
    codeRef: "src/analytics/dashboard_service.py -> DashboardService",
    samplePayload: {
      "total_activities": 19,
      "videos": 3,
      "shorts": 5,
      "posts": 6,
      "live": 5,
      "total_processing_seconds": 1284.5
    }
  },
  "safe_deletion": {
    name: "Safe Deletion & Audit Service",
    category: "Management",
    type: "GLOBAL",
    color: "#ef4444",
    description: "Controlled two-step deletion service: preview generation with exact artifact counts, explicit confirmation token, cascading file purge, and audit logging.",
    inputs: "Scope, channel list, date range, activity types, CONFIRM_DELETE token",
    outputs: "Deleted counts across 8 artifact categories, audit record",
    filesProduced: "logs/data_management/audit.jsonl, data/analytics/previews/{id}.json",
    timingTrackers: "started_at, completed_at",
    failureModes: "Token mismatch, preview expired, file locked",
    retryPolicy: "Atomic file removal; skips missing files cleanly",
    codeRef: "src/storage/deletion_service.py -> DeletionService",
    samplePayload: {
      "audit_id": "audit_8492048f",
      "scope": "CHANNEL_DATE_RANGE",
      "items_deleted": 6,
      "artifacts_deleted": 19,
      "user_confirmed": true,
      "status": "COMPLETED"
    }
  }
};

// Application State
let currentTab = "full";
let currentZoom = 1.0;
let panX = 40;
let panY = 40;
let isPanning = false;
let startX = 0;
let startY = 0;

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initControls();
  initSearch();
  initPanZoom();
  renderCurrentDiagram();
});

function initTabs() {
  const tabBtns = document.querySelectorAll(".tab-btn");
  tabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      tabBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentTab = btn.getAttribute("data-tab");
      resetPanZoom();
      renderCurrentDiagram();
    });
  });
}

function initControls() {
  document.getElementById("btn-zoom-in").addEventListener("click", () => {
    currentZoom = Math.min(2.5, currentZoom + 0.2);
    applyTransform();
  });

  document.getElementById("btn-zoom-out").addEventListener("click", () => {
    currentZoom = Math.max(0.4, currentZoom - 0.2);
    applyTransform();
  });

  document.getElementById("btn-reset").addEventListener("click", () => {
    resetPanZoom();
  });

  document.getElementById("drawer-close").addEventListener("click", () => {
    closeDrawer();
  });
}

function resetPanZoom() {
  currentZoom = 1.0;
  panX = 40;
  panY = 40;
  applyTransform();
}

function applyTransform() {
  const canvas = document.getElementById("diagram-canvas");
  if (canvas) {
    canvas.style.transform = `translate(${panX}px, ${panY}px) scale(${currentZoom})`;
  }
}

function initPanZoom() {
  const container = document.getElementById("viewport-container");

  container.addEventListener("mousedown", (e) => {
    if (e.target.closest(".node-group")) return;
    isPanning = true;
    startX = e.clientX - panX;
    startY = e.clientY - panY;
  });

  window.addEventListener("mousemove", (e) => {
    if (!isPanning) return;
    panX = e.clientX - startX;
    panY = e.clientY - startY;
    applyTransform();
  });

  window.addEventListener("mouseup", () => {
    isPanning = false;
  });

  container.addEventListener("wheel", (e) => {
    e.preventDefault();
    const zoomDelta = e.deltaY < 0 ? 0.1 : -0.1;
    currentZoom = Math.min(2.5, Math.max(0.4, currentZoom + zoomDelta));
    applyTransform();
  }, { passive: false });
}

function initSearch() {
  const input = document.getElementById("search-input");
  input.addEventListener("input", (e) => {
    const q = e.target.value.toLowerCase().trim();
    const nodes = document.querySelectorAll(".node-group");
    nodes.forEach(n => {
      const stageKey = n.getAttribute("data-stage");
      const spec = STAGE_SPECS[stageKey];
      if (!q) {
        n.querySelector(".node-box").classList.remove("highlight");
      } else if (spec && (spec.name.toLowerCase().includes(q) || spec.description.toLowerCase().includes(q))) {
        n.querySelector(".node-box").classList.add("highlight");
      } else {
        n.querySelector(".node-box").classList.remove("highlight");
      }
    });
  });
}

function openDrawer(stageKey) {
  const spec = STAGE_SPECS[stageKey];
  if (!spec) return;

  const drawer = document.getElementById("detail-drawer");
  document.getElementById("drawer-stage-title").innerText = spec.name;
  
  const badge = document.getElementById("drawer-stage-badge");
  badge.innerText = spec.type;
  badge.style.backgroundColor = spec.color + "33";
  badge.style.color = spec.color;
  badge.style.border = `1px solid ${spec.color}`;

  document.getElementById("drawer-desc").innerText = spec.description;
  document.getElementById("drawer-input").innerText = spec.inputs;
  document.getElementById("drawer-output").innerText = spec.outputs;
  document.getElementById("drawer-files").innerText = spec.filesProduced;
  document.getElementById("drawer-timing").innerText = spec.timingTrackers;
  document.getElementById("drawer-failure").innerText = spec.failureModes;
  document.getElementById("drawer-retry").innerText = spec.retryPolicy;
  document.getElementById("drawer-code").innerText = spec.codeRef;
  document.getElementById("drawer-payload").innerText = JSON.stringify(spec.samplePayload, null, 2);

  drawer.classList.add("open");
}

function closeDrawer() {
  document.getElementById("detail-drawer").classList.remove("open");
}

function renderCurrentDiagram() {
  const canvas = document.getElementById("diagram-canvas");
  if (!canvas) return;

  if (currentTab === "full") {
    canvas.innerHTML = renderFullPipelineSVG();
  } else if (currentTab === "flows") {
    canvas.innerHTML = renderFourFlowsSVG();
  } else if (currentTab === "evidence") {
    canvas.innerHTML = renderEvidenceCitationSVG();
  } else if (currentTab === "profiling") {
    canvas.innerHTML = renderProcessingTimeSVG();
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
}

// -------------------------------------------------------------
// SVG Diagram Builders
// -------------------------------------------------------------

function renderFullPipelineSVG() {
  return `
  <svg width="1450" height="980" viewBox="0 0 1450 980" xmlns="http://www.w3.org/2000/svg">
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
    </defs>

    <!-- Stage 1: Discovery & Ingestion -->
    ${svgNode(40, 100, 200, 70, "watchlist", "Watchlist Config", "channels.yaml", "#3b82f6", "GLOBAL")}
    ${svgEdge(240, 135, 300, 135, "arrow")}

    ${svgNode(300, 100, 220, 70, "discovery", "Activity Discovery", "yt-dlp multi-tab scanner", "#3b82f6", "STAGE 1")}
    ${svgEdge(520, 135, 580, 135, "arrow")}

    ${svgNode(580, 100, 220, 70, "dedup", "Deduplication Check", "State & hash verification", "#8b5cf6", "GATE")}
    
    <!-- Fork into 4 branches -->
    <!-- Video branch -->
    ${svgEdge(800, 135, 870, 70, "arrow-blue")}
    ${svgNode(870, 40, 220, 60, "download", "Download Audio", "Opus audio extraction", "#3b82f6", "VIDEO")}

    <!-- Short branch -->
    ${svgEdge(800, 135, 870, 150, "arrow-pink")}
    ${svgNode(870, 120, 220, 60, "download", "Download Short Audio", "Opus audio stream", "#ec4899", "SHORT")}

    <!-- Post branch -->
    ${svgEdge(800, 135, 870, 230, "arrow-purple")}
    ${svgNode(870, 200, 220, 60, "post_text", "Direct Post Text", "Zero audio needed", "#8b5cf6", "POST")}

    <!-- Live branch -->
    ${svgEdge(800, 135, 870, 310, "arrow-red")}
    ${svgNode(870, 280, 220, 60, "download", "Live Stream Segmenter", "Incremental chunk acquisition", "#ef4444", "LIVE")}

    <!-- Transcription for Media streams -->
    ${svgEdge(1090, 70, 1160, 105, "arrow")}
    ${svgEdge(1090, 150, 1160, 105, "arrow")}
    ${svgEdge(1090, 310, 1160, 105, "arrow")}
    ${svgNode(1160, 75, 230, 65, "transcription", "Faster-Whisper CUDA", "Hindi / Hinglish / English VAD", "#10b981", "SPEECH")}

    <!-- Convergence to LLM & Evidence Grounding -->
    ${svgEdge(1275, 140, 1275, 400, "arrow")}
    ${svgEdge(1090, 230, 1200, 400, "arrow-purple")}

    <!-- Row 2: Analysis & Grounding -->
    ${svgNode(1160, 400, 240, 75, "llm_analysis", "Ollama LLM Engine", "gemma3:12b / qwen3:8b JSON", "#f59e0b", "ANALYSIS")}
    ${svgEdge(1160, 437, 1050, 437, "arrow")}

    ${svgNode(820, 400, 230, 75, "evidence_validation", "Evidence Validation", "Levenshtein Fuzzy Grounding", "#10b981", "GROUNDING")}
    ${svgEdge(820, 437, 720, 437, "arrow")}

    ${svgNode(490, 400, 230, 75, "citation_generation", "Deep Citation Linking", "Timestamp & Post Permalinks", "#06b6d4", "CITATIONS")}
    ${svgEdge(490, 437, 390, 437, "arrow")}

    ${svgNode(150, 400, 240, 75, "summary_synthesis", "Grounded Summaries", "Executive Summary & Key Points", "#3b82f6", "SYNTHESIS")}

    <!-- Row 3: Reporting & Storage -->
    ${svgEdge(270, 475, 270, 560, "arrow")}
    ${svgNode(150, 560, 240, 75, "reports", "Report Generator", "HTML & JSON compilation", "#8b5cf6", "REPORTS")}
    ${svgEdge(390, 597, 490, 597, "arrow")}

    ${svgNode(490, 560, 240, 75, "timing_profiler", "Timing Profiler", "StageTimings & Wall Clock", "#f59e0b", "PROFILER")}
    ${svgEdge(730, 597, 820, 597, "arrow")}

    <!-- Row 4: Analytics & Dashboard -->
    ${svgNode(820, 560, 250, 75, "analytics_aggregator", "Dashboard Aggregator", "Dynamic Date-Range Intelligence", "#06b6d4", "ANALYTICS")}
    ${svgEdge(1070, 597, 1150, 597, "arrow")}

    ${svgNode(1150, 560, 240, 75, "safe_deletion", "Safe Deletion Hub", "2-Step Preview, Token & Audit", "#ef4444", "MANAGEMENT")}

    <!-- Visual Legend embedded -->
    <rect x="40" y="700" width="1360" height="230" rx="8" fill="#1e293b" stroke="#334155" stroke-width="1.5" />
    <text x="60" y="730" fill="#f8fafc" font-size="14" font-weight="700">PIPELINE ARCHITECTURE SPECIFICATIONS & STAGES</text>
    
    <text x="60" y="760" fill="#94a3b8" font-size="12">• Local-First: Zero cloud APIs. All computation executes locally on workstation NVMe and RTX 4080 (16GB VRAM).</text>
    <text x="60" y="785" fill="#94a3b8" font-size="12">• Strict Evidence Grounding: LLM outputs are verified against raw text. Unsubstantiated claims are discarded.</text>
    <text x="60" y="810" fill="#94a3b8" font-size="12">• Microsecond Profiling: Every individual stage duration is timed via perf_counter and persisted in ActivityTiming JSON.</text>
    <text x="60" y="835" fill="#94a3b8" font-size="12">• Destructive Safety: Deletion requires exact preview verification and explicit confirmation token 'CONFIRM_DELETE'.</text>
    <text x="60" y="860" fill="#94a3b8" font-size="12">• Click any stage node above to inspect input/output schemas, generated files, error handling, and code references.</text>
  </svg>
  `;
}

function renderFourFlowsSVG() {
  return `
  <svg width="1380" height="880" viewBox="0 0 1380 880" xmlns="http://www.w3.org/2000/svg">
    <!-- VIDEO SWIMLANE -->
    <rect x="30" y="40" width="1320" height="170" rx="8" fill="#1e293b" stroke="#3b82f6" stroke-width="1.5" stroke-opacity="0.6"/>
    <text x="50" y="70" fill="#3b82f6" font-size="14" font-weight="800">1. LONG-FORM VIDEO PIPELINE</text>
    ${svgNode(60, 95, 170, 50, "discovery", "Discovery", "yt-dlp video tab", "#3b82f6", "STEP 1")}
    ${svgEdge(230, 120, 270, 120, "arrow")}
    ${svgNode(270, 95, 170, 50, "download", "Download Audio", "Opus Stream", "#3b82f6", "STEP 2")}
    ${svgEdge(440, 120, 480, 120, "arrow")}
    ${svgNode(480, 95, 180, 50, "transcription", "Faster-Whisper", "16k WAV + VAD", "#10b981", "STEP 3")}
    ${svgEdge(660, 120, 700, 120, "arrow")}
    ${svgNode(700, 95, 180, 50, "chunking", "Chunking", "Semantic windows", "#06b6d4", "STEP 4")}
    ${svgEdge(880, 120, 920, 120, "arrow")}
    ${svgNode(920, 95, 180, 50, "llm_analysis", "Ollama LLM", "Multi-chunk fusion", "#f59e0b", "STEP 5")}
    ${svgEdge(1100, 120, 1140, 120, "arrow")}
    ${svgNode(1140, 95, 180, 50, "citation_generation", "Deep Citations", "&t=XXs links", "#06b6d4", "STEP 6")}

    <!-- SHORT SWIMLANE -->
    <rect x="30" y="240" width="1320" height="170" rx="8" fill="#1e293b" stroke="#ec4899" stroke-width="1.5" stroke-opacity="0.6"/>
    <text x="50" y="270" fill="#ec4899" font-size="14" font-weight="800">2. YOUTUBE SHORTS PIPELINE</text>
    ${svgNode(60, 295, 170, 50, "discovery", "Discovery", "yt-dlp shorts tab", "#ec4899", "STEP 1")}
    ${svgEdge(230, 320, 280, 320, "arrow")}
    ${svgNode(280, 295, 180, 50, "download", "Download Audio", "Short-duration opus", "#ec4899", "STEP 2")}
    ${svgEdge(460, 320, 510, 320, "arrow")}
    ${svgNode(510, 295, 190, 50, "transcription", "Faster-Whisper", "Single-pass transcribe", "#10b981", "STEP 3")}
    ${svgEdge(700, 320, 750, 320, "arrow")}
    ${svgNode(750, 295, 190, 50, "llm_analysis", "Ollama LLM", "Concise single prompt", "#f59e0b", "STEP 4")}
    ${svgEdge(940, 320, 990, 320, "arrow")}
    ${svgNode(990, 295, 190, 50, "reports", "Shorts Report", "Single item report", "#8b5cf6", "STEP 5")}

    <!-- COMMUNITY POST SWIMLANE -->
    <rect x="30" y="440" width="1320" height="170" rx="8" fill="#1e293b" stroke="#8b5cf6" stroke-width="1.5" stroke-opacity="0.6"/>
    <text x="50" y="470" fill="#8b5cf6" font-size="14" font-weight="800">3. COMMUNITY POST PIPELINE (AUDIO-FREE)</text>
    ${svgNode(60, 495, 170, 50, "discovery", "Discovery", "yt-dlp community tab", "#8b5cf6", "STEP 1")}
    ${svgEdge(230, 520, 320, 520, "arrow")}
    ${svgNode(320, 495, 220, 50, "post_text", "Direct Text Normalizer", "Skips audio/whisper", "#8b5cf6", "STEP 2")}
    ${svgEdge(540, 520, 630, 520, "arrow")}
    ${svgNode(630, 495, 220, 50, "llm_analysis", "Ollama LLM", "Text-only extraction", "#f59e0b", "STEP 3")}
    ${svgEdge(850, 520, 940, 520, "arrow")}
    ${svgNode(940, 495, 220, 50, "citation_generation", "Post Permalinks", "Exact post URL & text", "#06b6d4", "STEP 4")}

    <!-- LIVE STREAM SWIMLANE -->
    <rect x="30" y="640" width="1320" height="170" rx="8" fill="#1e293b" stroke="#ef4444" stroke-width="1.5" stroke-opacity="0.6"/>
    <text x="50" y="670" fill="#ef4444" font-size="14" font-weight="800">4. YOUTUBE LIVE PIPELINE (STREAMING)</text>
    ${svgNode(60, 695, 170, 50, "discovery", "Live Discovery", "yt-dlp live badge scan", "#ef4444", "STEP 1")}
    ${svgEdge(230, 720, 270, 720, "arrow")}
    ${svgNode(270, 695, 180, 50, "download", "Live Segment Ingest", "Rolling audio chunks", "#ef4444", "STEP 2")}
    ${svgEdge(450, 720, 490, 720, "arrow")}
    ${svgNode(490, 695, 190, 50, "transcription", "Incremental Whisper", "Sliding window VAD", "#10b981", "STEP 3")}
    ${svgEdge(680, 720, 720, 720, "arrow")}
    ${svgNode(720, 695, 190, 50, "llm_analysis", "Incremental Analysis", "Continuous entity log", "#f59e0b", "STEP 4")}
    ${svgEdge(910, 720, 950, 720, "arrow")}
    ${svgNode(950, 695, 200, 50, "reports", "Final Live Summary", "End-of-stream consolidation", "#8b5cf6", "STEP 5")}
  </svg>
  `;
}

function renderEvidenceCitationSVG() {
  return `
  <svg width="1280" height="700" viewBox="0 0 1280 700" xmlns="http://www.w3.org/2000/svg">
    <!-- Evidence Grounding Architecture -->
    ${svgNode(50, 80, 240, 70, "transcription", "Raw Timestamped Transcript", "Word-level millisecond timing", "#10b981", "INPUT 1")}
    ${svgNode(50, 200, 240, 70, "post_text", "Raw Post Text", "Original post content", "#8b5cf6", "INPUT 2")}

    ${svgEdge(290, 115, 380, 155, "arrow")}
    ${svgEdge(290, 235, 380, 155, "arrow")}

    ${svgNode(380, 120, 250, 75, "llm_analysis", "Candidate Statement Extraction", "Model proposes claim + quote", "#f59e0b", "STEP 1")}
    ${svgEdge(630, 157, 720, 157, "arrow")}

    ${svgNode(720, 120, 250, 75, "evidence_validation", "Levenshtein Fuzzy Grounding", "Verified if match ratio >= 0.85", "#10b981", "STEP 2")}
    
    <!-- Fork: Accepted vs Rejected -->
    ${svgEdge(845, 195, 845, 300, "arrow")}
    ${svgNode(720, 300, 250, 70, "summary_synthesis", "Evidence Object Created", "Assigned unique ID (ev_XXX)", "#3b82f6", "ACCEPTED")}

    ${svgEdge(970, 157, 1060, 157, "arrow")}
    ${svgNode(1060, 120, 180, 75, "safe_deletion", "Discard Statement", "Hallucinated / Unverifiable", "#ef4444", "REJECTED")}

    <!-- Citation Generation -->
    ${svgEdge(845, 370, 845, 450, "arrow")}
    ${svgNode(720, 450, 250, 75, "citation_generation", "Deep Citation Link Created", "Timestamp: &t=XXs / Post URL", "#06b6d4", "STEP 3")}

    ${svgEdge(720, 487, 580, 487, "arrow")}
    ${svgNode(330, 450, 250, 75, "summary_synthesis", "Executive Summary Grounding", "Summary claims link to evidence", "#3b82f6", "SYNTHESIS")}
  </svg>
  `;
}

function renderProcessingTimeSVG() {
  return `
  <svg width="1280" height="720" viewBox="0 0 1280 720" xmlns="http://www.w3.org/2000/svg">
    <!-- Profiling Flow -->
    ${svgNode(50, 80, 240, 70, "discovery", "Stage Entry Hook", "t0 = time.perf_counter()", "#3b82f6", "HOOK START")}
    ${svgEdge(290, 115, 380, 115, "arrow")}

    ${svgNode(380, 80, 240, 70, "transcription", "Stage Execution", "Processing stage executes", "#10b981", "RUNNING")}
    ${svgEdge(620, 115, 710, 115, "arrow")}

    ${svgNode(710, 80, 240, 70, "timing_profiler", "Stage Exit Hook", "t1 = time.perf_counter()", "#f59e0b", "HOOK END")}
    ${svgEdge(830, 150, 830, 230, "arrow")}

    ${svgNode(710, 230, 240, 80, "timing_profiler", "StageTiming Record", "duration = round(t1 - t0, 3)", "#f59e0b", "METRIC")}
    ${svgEdge(710, 270, 570, 270, "arrow")}

    ${svgNode(330, 230, 240, 80, "timing_profiler", "ActivityTiming Aggregator", "Wall-clock vs Stage Sums", "#8b5cf6", "ACTIVITY PROFILE")}
    ${svgEdge(330, 270, 190, 270, "arrow")}

    ${svgNode(50, 230, 240, 80, "analytics_aggregator", "Persistent Timing Storage", "data/analytics/timing/{id}.json", "#06b6d4", "FILE STORE")}

    <!-- Visual Breakdown Sample -->
    <rect x="50" y="380" width="1180" height="280" rx="8" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="80" y="415" fill="#f8fafc" font-size="14" font-weight="700">REAL-TIME PROFILER HORIZONTAL TIMELINE BREAKDOWN</text>
    
    <text x="80" y="450" fill="#94a3b8" font-size="12">Discovery (1.4s)</text>
    <rect x="220" y="438" width="28" height="16" rx="3" fill="#3b82f6"/>

    <text x="80" y="480" fill="#94a3b8" font-size="12">Download (8.5s)</text>
    <rect x="220" y="468" width="120" height="16" rx="3" fill="#ec4899"/>

    <text x="80" y="510" fill="#94a3b8" font-size="12">Transcription (29.2s)</text>
    <rect x="220" y="498" width="380" height="16" rx="3" fill="#10b981"/>

    <text x="80" y="540" fill="#94a3b8" font-size="12">LLM Analysis (18.4s)</text>
    <rect x="220" y="528" width="240" height="16" rx="3" fill="#f59e0b"/>

    <text x="80" y="570" fill="#94a3b8" font-size="12">Evidence & Citations (1.8s)</text>
    <rect x="220" y="558" width="35" height="16" rx="3" fill="#06b6d4"/>

    <text x="80" y="600" fill="#94a3b8" font-size="12">Report Generation (0.7s)</text>
    <rect x="220" y="588" width="18" height="16" rx="3" fill="#8b5cf6"/>

    <text x="80" y="635" fill="#f8fafc" font-size="13" font-weight="700">Total Wall-Clock Elapsed: 60.0s | Whisper CUDA: 29.2s | Ollama LLM: 18.4s</text>
  </svg>
  `;
}

function renderStorageHierarchySVG() {
  return `
  <svg width="1280" height="720" viewBox="0 0 1280 720" xmlns="http://www.w3.org/2000/svg">
    <!-- Root -->
    <rect x="50" y="40" width="220" height="50" rx="6" fill="#0f172a" stroke="#3b82f6" stroke-width="2"/>
    <text x="70" y="70" fill="#f8fafc" font-size="14" font-weight="700">📁 data/ (Root NVMe)</text>

    <!-- Branch 1: Activities -->
    ${svgEdge(160, 90, 160, 160, "arrow")}
    <rect x="70" y="160" width="280" height="140" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="90" y="185" fill="#3b82f6" font-size="13" font-weight="700">📁 activities/YYYY/MM/DD/</text>
    <text x="110" y="210" fill="#94a3b8" font-size="11">├── 📁 videos/{id}.json</text>
    <text x="110" y="235" fill="#94a3b8" font-size="11">├── 📁 shorts/{id}.json</text>
    <text x="110" y="260" fill="#94a3b8" font-size="11">├── 📁 posts/{id}.json</text>
    <text x="110" y="285" fill="#94a3b8" font-size="11">└── 📁 live/{id}.json</text>

    <!-- Branch 2: Transcripts -->
    ${svgEdge(270, 65, 420, 65, "arrow")}
    <rect x="420" y="40" width="240" height="100" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="440" y="65" fill="#10b981" font-size="13" font-weight="700">📁 transcripts/</text>
    <text x="460" y="90" fill="#94a3b8" font-size="11">└── {item_id}.json</text>
    <text x="460" y="115" fill="#64748b" font-size="10">Word/segment timestamps</text>

    <!-- Branch 3: Analysis -->
    ${svgEdge(540, 140, 540, 190, "arrow")}
    <rect x="420" y="190" width="240" height="100" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="440" y="215" fill="#f59e0b" font-size="13" font-weight="700">📁 analysis/</text>
    <text x="460" y="240" fill="#94a3b8" font-size="11">└── {item_id}.json</text>
    <text x="460" y="265" fill="#64748b" font-size="10">Topics, claims, evidence catalog</text>

    <!-- Branch 4: Reports -->
    ${svgEdge(660, 65, 750, 65, "arrow")}
    <rect x="750" y="40" width="260" height="140" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="770" y="65" fill="#8b5cf6" font-size="13" font-weight="700">📁 reports/</text>
    <text x="790" y="90" fill="#94a3b8" font-size="11">├── 📁 items/{id}.html & .json</text>
    <text x="790" y="115" fill="#94a3b8" font-size="11">├── 📁 daily/{date}.html & .json</text>
    <text x="790" y="140" fill="#94a3b8" font-size="11">└── 📁 channel/{ch_id}/{date}.json</text>

    <!-- Branch 5: Analytics -->
    ${svgEdge(880, 180, 880, 220, "arrow")}
    <rect x="750" y="220" width="260" height="110" rx="6" fill="#1e293b" stroke="#334155" stroke-width="1.5"/>
    <text x="770" y="245" fill="#06b6d4" font-size="13" font-weight="700">📁 analytics/</text>
    <text x="790" y="270" fill="#94a3b8" font-size="11">├── 📁 timing/{id}.json</text>
    <text x="790" y="295" fill="#94a3b8" font-size="11">└── 📁 previews/{id}.json</text>

    <!-- Branch 6: Audit Logs -->
    ${svgEdge(1010, 65, 1080, 65, "arrow")}
    <rect x="1080" y="40" width="180" height="100" rx="6" fill="#1e293b" stroke="#ef4444" stroke-width="1.5"/>
    <text x="1100" y="65" fill="#ef4444" font-size="13" font-weight="700">📁 logs/</text>
    <text x="1110" y="90" fill="#94a3b8" font-size="11">└── audit.jsonl</text>
    <text x="1110" y="115" fill="#64748b" font-size="10">Deletion audit trail</text>
  </svg>
  `;
}

function renderDeletionAuditSVG() {
  return `
  <svg width="1280" height="700" viewBox="0 0 1280 700" xmlns="http://www.w3.org/2000/svg">
    <!-- Deletion Lifecycle -->
    ${svgNode(50, 100, 240, 75, "safe_deletion", "1. User Criteria Selection", "Channel, Date, Range, Type", "#3b82f6", "REQUEST")}
    ${svgEdge(290, 137, 380, 137, "arrow")}

    ${svgNode(380, 100, 250, 75, "safe_deletion", "2. Preview Calculation", "Scans matching files, ZERO deletions", "#06b6d4", "PREVIEW")}
    ${svgEdge(630, 137, 720, 137, "arrow")}

    ${svgNode(720, 100, 260, 75, "safe_deletion", "3. Affected Counts Modal", "Displays exact count of 8 artifacts", "#f59e0b", "REVIEW")}
    ${svgEdge(850, 175, 850, 250, "arrow")}

    ${svgNode(720, 250, 260, 75, "safe_deletion", "4. Explicit Token Gate", "Requires token: 'CONFIRM_DELETE'", "#ef4444", "CONFIRMATION")}
    ${svgEdge(720, 287, 600, 287, "arrow")}

    ${svgNode(340, 250, 260, 75, "safe_deletion", "5. Cascading Artifact Deletion", "Deletes activity, audio, transcript, report", "#ef4444", "EXECUTION")}
    ${svgEdge(340, 287, 220, 287, "arrow")}

    ${svgNode(50, 250, 240, 75, "safe_deletion", "6. Audit Log Recorded", "Appends to logs/data_management/audit.jsonl", "#10b981", "AUDIT")}
    ${svgEdge(170, 325, 170, 420, "arrow")}

    ${svgNode(50, 420, 240, 75, "analytics_aggregator", "7. Cache / Report Refresh", "Recalculates or cleans daily reports", "#06b6d4", "SYNC")}
  </svg>
  `;
}

// -------------------------------------------------------------
// SVG Helper Generators
// -------------------------------------------------------------

function svgNode(x, y, w, h, stageKey, title, subtitle, color, badgeText) {
  return `
  <g class="node-group" data-stage="${stageKey}" transform="translate(${x}, ${y})">
    <rect class="node-box" width="${w}" height="${h}" rx="8" />
    <rect x="12" y="10" width="8" height="${h - 20}" rx="4" fill="${color}" />
    <text class="node-title" x="30" y="28">${title}</text>
    <text class="node-subtitle" x="30" y="45">${subtitle}</text>
    ${badgeText ? `
      <rect x="${w - 70}" y="10" width="58" height="18" rx="4" fill="${color}22" stroke="${color}" stroke-width="1" />
      <text class="node-badge" x="${w - 41}" y="22" fill="${color}" text-anchor="middle">${badgeText}</text>
    ` : ""}
  </g>
  `;
}

function svgEdge(x1, y1, x2, y2, markerId) {
  const dx = x2 - x1;
  const dy = y2 - y1;
  // Bezier curve
  const cx1 = x1 + dx * 0.5;
  const cy1 = y1;
  const cx2 = x1 + dx * 0.5;
  const cy2 = y2;

  return `
  <path class="edge-path flow-active" d="M ${x1} ${y1} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${x2} ${y2}" marker-end="url(#${markerId || "arrow"})" />
  `;
}
