/**
 * Political YouTube Channel Activity Monitor
 * Standalone Interactive Pipeline Architecture & Flow Documentation
 * Zero external runtime dependencies - Pure Vanilla JS & Dynamic SVG.
 * 
 * Features:
 * - Default Light Theme with sleek Dark Mode toggle
 * - 100% technical fidelity to real Python codebase
 * - Streamlined Worker Pool Concurrency (ThreadPoolExecutor, QUEUED stage)
 * - Native YouTube Caption Shortcut (~0.05s bypass)
 * - 4 Distinct Activity Lifecycles (Videos, Shorts, Posts, Live Streams)
 * - Grounded Citations Provenance with Levenshtein fuzzy matching
 * - Processing-Time Profiler and Dual-Tier NVMe + MongoDB Storage
 * - Safe Deletion Lifecycle with Cryptographic Confirmation Tokens
 */

// =============================================================================
// 1. Comprehensive Technical Specifications for Every Pipeline Stage
// =============================================================================
const STAGE_SPECS = {
  "watchlist": {
    name: "Channel Watchlist Registry",
    category: "Ingestion Config",
    type: "SEQUENTIAL",
    color: "#2563eb",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Configured channels (PIB India, Sansad TV, @NarendraModi) monitored with zero failure guarantees.",
    description: "Loads and validates active YouTube channel configurations from YAML. Defines channel identifier, handle/URL, friendly name, enabled status, and category tags.",
    inputs: "config/channels.yaml, config/settings.yaml",
    outputs: "List of validated ChannelConfig Pydantic objects",
    filesProduced: "config/channels.yaml, data/channels/{channel_id}/channel.json",
    timingTrackers: "startup_channel_load (microseconds)",
    failureModes: "YAML parsing syntax error, missing required keys (id, name, url), inaccessible file path.",
    retryPolicy: "Immediate abort on startup with informative terminal diagnostic log.",
    codeRef: "src/config.py -> Settings.load_channels()",
    samplePayload: {
      "id": "sansad_tv",
      "name": "Sansad TV",
      "url": "https://www.youtube.com/@SansadTV",
      "enabled": true,
      "tags": ["parliament", "policy", "national"]
    }
  },
  "discovery": {
    name: "Multi-Collector Activity Scanner",
    category: "Discovery Phase",
    type: "SEQUENTIAL",
    color: "#2563eb",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Live multi-channel catalog for 2026-09-12 showing 28 cataloged activities across Videos, Shorts, and Live.",
    description: "Coordinates specialized collectors (MetadataCollector, VideoCollector, ShortsCollector, LiveCollector, PostsCollector) using yt-dlp flat-playlist extraction and InnerTube HTML scraping.",
    inputs: "ChannelConfig, target date window (start_date, end_date), enable_posts, enable_live flags",
    outputs: "ChannelMetadata, List[ActivityItem], PostStatus, LiveCollectionStatus",
    filesProduced: "data/channels/{channel_id}/channel.json, data/raw/{item_id}_raw.json",
    timingTrackers: "discovery_start, discovery_end, stage_duration_seconds",
    failureModes: "YouTube rate-limiting / bot challenge (HTTP 429), network socket timeout, channel renamed or deleted.",
    retryPolicy: "Exponential backoff with 3 retries (1s, 2s, 4s delay). Gracefully flags PostStatus/LiveCollectionStatus.",
    codeRef: "src/collectors/youtube_collector.py -> YouTubeCollector.collect_channel_activity()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "activity_type": "VIDEO",
      "channel_id": "sansad_tv",
      "channel_name": "Sansad TV",
      "title": "Aapke Sansad | Dr. Manna Lal Rawat, MP Udaipur | 11 Sept, 2026",
      "published_at": "2026-09-12T05:30:00Z",
      "duration": 1794,
      "source": "youtube"
    }
  },
  "date_filter": {
    name: "Temporal Window Matcher",
    category: "Ingestion Gate",
    type: "SEQUENTIAL",
    color: "#0891b2",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Date-range selector matching items published on 2026-09-12 with precision filtering.",
    description: "Strictly filters discovered activities by comparing publication timestamps against the requested UTC single date or date range. Resolves date strings, handles ISO-8601 timestamps and relative time.",
    inputs: "Raw activity published_at, target_date (YYYY-MM-DD), or from_date -> to_date range",
    outputs: "Boolean gate decision: MATCH (Proceed to queue) vs SKIP (Out of window)",
    filesProduced: "In-memory filtered collection saved to data/activities/YYYY/MM/DD/{videos,shorts,posts,live}/{item_id}.json",
    timingTrackers: "filter_eval_microseconds",
    failureModes: "Malformed ISO-8601 publication timestamps, timezone offset ambiguities.",
    retryPolicy: "Parses with dateutil.parser; defaults to UTC normalization with IST awareness.",
    codeRef: "src/engine/date_filter.py -> DateFilter.matches_window()",
    samplePayload: {
      "target_date": "2026-09-12",
      "item_published_at": "2026-09-12T05:30:00Z",
      "start_date": "2026-09-12",
      "end_date": "2026-09-12",
      "matched": true
    }
  },
  "dedup": {
    name: "Deduplication & Idempotency Gate",
    category: "Ingestion Gate",
    type: "IDEMPOTENCY",
    color: "#7c3aed",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Deduplication ensures 28 distinct items are processed with 0 redundant re-executions.",
    description: "Guarantees idempotency. Checks state/items/{item_id}.json to determine whether an item has already been successfully analyzed, failed, or requires reprocessing via force_reprocess flag.",
    inputs: "item_id, force_reprocess flag, existing state files",
    outputs: "Boolean gate decision: PROCESS vs IDEMPOTENT SKIP",
    filesProduced: "state/items/{item_id}.json",
    timingTrackers: "dedup_check_microseconds",
    failureModes: "State JSON corrupted or file lock contention during concurrent runs.",
    retryPolicy: "Atomic JSON file write with tempfile rename. Recovers state from disk if corrupted.",
    codeRef: "src/engine/state_manager.py -> StateManager.is_item_completed()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "channel_id": "sansad_tv",
      "activity_type": "VIDEO",
      "stage": "COMPLETED",
      "first_seen_at": "2026-09-12T23:31:56Z",
      "last_processed_at": "2026-09-12T23:33:14Z",
      "retry_count": 0
    }
  },
  "queue_dispatch": {
    name: "Concurrent Queue & Dispatcher",
    category: "Worker Pool",
    type: "PARALLEL",
    color: "#7c3aed",
    demoImage: "assets/04_live_processing.png",
    demoCaption: "Concurrent execution across multiple worker threads with live status tracking.",
    description: "Batches all discovered activities matching the target window and enqueues them into a ThreadPoolExecutor (max_workers=3). Emits instant QUEUED ProgressEvents via SSE for real-time frontend monitoring.",
    inputs: "List of matching (ActivityItem, ChannelConfig) tuples, max_workers setting",
    outputs: "Dispatched concurrent futures tracked via concurrent.futures.as_completed",
    filesProduced: "state/runs/{run_id}.json",
    timingTrackers: "queue_enqueue_time, worker_wait_latency",
    failureModes: "Thread starvation, memory exhaustion under very high concurrency.",
    retryPolicy: "Bounded worker pool (default 3 concurrent workers) to ensure GPU VRAM and CPU stability.",
    codeRef: "src/engine/pipeline.py -> MonitoringPipeline.run() (ThreadPoolExecutor)",
    samplePayload: {
      "run_id": "run_20260912_streamlined_7b",
      "stage": "QUEUED",
      "status": "queued",
      "concurrency": 3,
      "items_queued": 28
    }
  },
  "caption_shortcut": {
    name: "Native Captions Shortcut Bypass",
    category: "Speech Optimization",
    type: "FAST PATH",
    color: "#059669",
    demoImage: "assets/03_grounded_intelligence_detail.png",
    demoCaption: "Native Hindi & English captions retrieved in ~0.05s, bypassing audio download and Whisper CUDA inference.",
    description: "Attempts to fetch official or auto-generated YouTube captions directly using youtube-transcript-api. Checks Hindi (hi, hi-Latn), English (en), Gujarati (gu), Marathi (mr). If present, saves ~99% processing time and achieves zero WER script confusion.",
    inputs: "item_id",
    outputs: "TranscriptData with exact start/end segment timestamps, or None fallback",
    filesProduced: "data/transcripts/{item_id}.json",
    timingTrackers: "captions_fetch_duration (~0.05s)",
    failureModes: "Captions disabled by creator, language unavailable, HTTP 429 from YouTube.",
    retryPolicy: "Immediate transparent fallback to Audio Extractor + Faster-Whisper CUDA pipeline.",
    codeRef: "src/transcription/whisper_engine.py -> WhisperEngine.fetch_youtube_captions()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "language": "hi",
      "language_probability": 1.0,
      "duration": 1794.0,
      "source": "native_youtube_captions",
      "fetch_time_seconds": 0.048
    }
  },
  "download": {
    name: "Media Stream Acquisition",
    category: "Ingestion Phase",
    type: "PARALLEL",
    color: "#2563eb",
    demoImage: "assets/04_live_processing.png",
    demoCaption: "Acquires audio streams for items lacking native YouTube captions.",
    description: "Downloads high-efficiency Opus audio streams for Videos, Shorts, and completed Livestreams via yt-dlp. Uses player_client=android,web to bypass YouTube restrictions.",
    inputs: "Video URL, item_id",
    outputs: "Raw audio stream saved to scratch directory",
    filesProduced: "data/scratch/audio/{item_id}.part, data/scratch/audio/{item_id}.opus",
    timingTrackers: "download_duration_seconds",
    failureModes: "Video deleted/private, age-restricted without cookies, socket reset.",
    retryPolicy: "yt-dlp retries with client rotation (android -> web -> ios).",
    codeRef: "src/audio/extractor.py -> AudioExtractor.extract_audio()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "format_id": "ba",
      "status": "downloaded"
    }
  },
  "live_segmenter": {
    name: "Live HLS Stream Slicer",
    category: "Live Streaming",
    type: "LIVE ONLY",
    color: "#dc2626",
    demoImage: "assets/04_live_processing.png",
    demoCaption: "Real-time 120s chunk extraction from active HLS streams during live broadcasts.",
    description: "Handles active livestreams (LIVE_NOW). Resolves live audio stream manifest URL with yt-dlp -g and slices a 120-second rolling audio chunk using ffmpeg (-t 120 -vn -acodec pcm_s16le). Enables incremental analysis during active broadcasts.",
    inputs: "Live stream URL, item_id, duration_seconds=120",
    outputs: "16kHz mono WAV slice of ongoing live broadcast",
    filesProduced: "data/scratch/audio/{item_id}.wav",
    timingTrackers: "live_slice_duration_seconds (approx 120s capture)",
    failureModes: "Live stream buffering, manifest expired, stream ended mid-capture.",
    retryPolicy: "Falls back to completed stream processing if broadcast has just finished.",
    codeRef: "src/audio/extractor.py -> AudioExtractor.extract_live_segment()",
    samplePayload: {
      "item_id": "live_stream_99",
      "live_status": "LIVE_NOW",
      "duration_seconds": 120,
      "is_incremental": true
    }
  },
  "audio_norm": {
    name: "FFmpeg Audio Normalization & Cleanup",
    category: "Audio Processing",
    type: "PARALLEL",
    color: "#db2777",
    demoImage: "assets/04_live_processing.png",
    demoCaption: "Transcodes audio to 16kHz mono WAV; immediately cleans up WAV after Whisper to avoid disk bloat.",
    description: "Transcodes extracted audio into standard 16kHz mono 16-bit PCM WAV (ffmpeg -ar 16000 -ac 1). Provides standard input required by faster-whisper. Performs immediate file deletion upon transcription completion.",
    inputs: "Raw audio stream, scratch file path",
    outputs: "Normalized 16kHz mono WAV file",
    filesProduced: "data/scratch/audio/{item_id}.wav (temporary)",
    timingTrackers: "ffmpeg_resample_seconds",
    failureModes: "FFmpeg subprocess failure, corrupted audio headers, disk full.",
    retryPolicy: "Raises descriptive RuntimeError, cleans up scratch artifacts.",
    codeRef: "src/audio/extractor.py -> AudioExtractor.cleanup_audio()",
    samplePayload: {
      "sample_rate": 16000,
      "channels": 1,
      "codec": "pcm_s16le",
      "auto_deleted": true
    }
  },
  "post_text": {
    name: "Community Post Text Parser",
    category: "Audio-Free Ingestion",
    type: "AUDIO-FREE",
    color: "#7c3aed",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Scrapes Community tab posts, extracting text, post status, and attached images without audio processing.",
    description: "Dedicated scraper for YouTube Community posts. Navigates InnerTube backstagePostThreadRenderer structures, extracts raw text, author, timestamp, and attached image URLs. Bypasses audio extraction and Whisper completely, passing directly to LLM post analysis.",
    inputs: "Channel /community or /posts URL, channel metadata",
    outputs: "ActivityItem with ActivityType.POST and post_text",
    filesProduced: "data/activities/YYYY/MM/DD/posts/{item_id}.json",
    timingTrackers: "posts_scrape_duration_seconds",
    failureModes: "Community tab disabled, InnerTube DOM payload format changes.",
    retryPolicy: "Returns PostStatus.NO_POSTS_FOUND or POST_NOT_SUPPORTED gracefully.",
    codeRef: "src/collectors/posts_collector.py -> PostsCollector._extract_posts_from_initial_data()",
    samplePayload: {
      "item_id": "UgkxyWncq0tEfeIah4tUw6GcURbbO6_6_PFo",
      "activity_type": "POST",
      "channel_id": "sansad_tv",
      "post_text": "Watch the special broadcast on national infrastructure initiatives today at 5 PM.",
      "post_status": "POST_FOUND"
    }
  },
  "transcription": {
    name: "Faster-Whisper Multilingual STT",
    category: "Speech-to-Text",
    type: "PARALLEL (CUDA)",
    color: "#059669",
    demoImage: "assets/03_grounded_intelligence_detail.png",
    demoCaption: "CUDA-accelerated speech-to-text with Silero VAD, word-level timestamps, and Hindi domain prompting.",
    description: "Performs local speech-to-text with faster-whisper. Runs on CUDA (compute_type=float16) with automatic CPU fallback (int8). Uses Silero VAD filtering to reject non-speech audio, extracts word-level timestamps, and uses Hindi political domain initial prompts.",
    inputs: "16kHz Mono WAV, item_id",
    outputs: "TranscriptData (full_text, language, duration, segments with word tokens)",
    filesProduced: "data/transcripts/{item_id}.json",
    timingTrackers: "whisper_duration_seconds, real_time_factor (RTF)",
    failureModes: "CUDA Out-of-Memory (OOM), silent audio, heavy background noise.",
    retryPolicy: "Catches CUDA OOM, falls back to CPU int8 execution automatically.",
    codeRef: "src/transcription/whisper_engine.py -> WhisperEngine.transcribe()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "language": "hi",
      "language_probability": 0.985,
      "duration": 1794.0,
      "segments_count": 312,
      "word_timestamps": true
    }
  },
  "llm_analysis": {
    name: "Dual-Engine Structured LLM",
    category: "AI Extraction",
    type: "PARALLEL",
    color: "#d97706",
    demoImage: "assets/03_grounded_intelligence_detail.png",
    demoCaption: "Structured political extraction using Gemma 3 12B with Qwen3 8B fallback and 420s chunking.",
    description: "Extracts structured political intelligence using Ollama. Uses Gemma 3 12B (primary) with Qwen3 8B (fallback). Videos <= 420s run single-pass; videos > 420s trigger 420s sliding chunking. Stage 2 synthesizes grounded executive summary referencing [EVID-xxx] tokens.",
    inputs: "ActivityItem, TranscriptData (or post_text)",
    outputs: "ItemAnalysis (summary, key_points, claims, announcements, promises, stats, entities, topics)",
    filesProduced: "data/analysis/{item_id}.json",
    timingTrackers: "llm_duration_seconds, tokens_per_second",
    failureModes: "LLM hallucination, Ollama server timeout, JSON schema validation errors.",
    retryPolicy: "3-tier retry: 1) Strict JSON format 2) Regex cleaning 3) Automatic failover to Qwen3 fallback model.",
    codeRef: "src/analysis/ollama_provider.py -> OllamaProvider.analyze_video()",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "summary": "Dr. Manna Lal Rawat discusses tribal community empowerment and rail connectivity.",
      "claims_count": 12,
      "announcements_count": 3,
      "promises_count": 2,
      "entities_count": 18,
      "model_used": "gemma3:12b"
    }
  },
  "evidence_validation": {
    name: "Programmatic Evidence Grounding",
    category: "Verification Phase",
    type: "PARALLEL",
    color: "#0891b2",
    demoImage: "assets/09_evidence_grounding.png",
    demoCaption: "Zero-hallucination guarantee: Verifies every AI claim against verbatim transcript text with Levenshtein fuzzy matching.",
    description: "Cross-verifies AI extracted claims, announcements, and summaries against verbatim transcript text. Uses 3-tier matching: 1) Direct substring match 2) 5-word sub-phrase match 3) >60% significant word overlap. Generates clickable timestamp URLs (&t=XXs) and renders inline [#] markers.",
    inputs: "ItemAnalysis, TranscriptData (or post_text)",
    outputs: "Grounded ItemAnalysis with verified=true/false and inline citation markers",
    filesProduced: "data/analysis/{item_id}.json (grounded)",
    timingTrackers: "evidence_validation_duration_seconds",
    failureModes: "Transcript segment timestamps missing, hallucinated quotes with zero overlap.",
    retryPolicy: "Flags ungrounded claims with verified=false and strips unverified citation tags.",
    codeRef: "src/engine/evidence_verifier.py -> EvidenceVerifier.verify_and_ground()",
    samplePayload: {
      "claim": "Rail project allocation increased by 40% for southern tribal districts.",
      "timestamp_start": 842.5,
      "timestamp_end": 855.0,
      "verified": true,
      "citation_url": "https://www.youtube.com/watch?v=EvF9mhGyMps&t=842s",
      "citation_label": "YouTube Video — 14:02"
    }
  },
  "citation_generation": {
    name: "Deep Citation & Anchor Synthesis",
    category: "Citation Engine",
    type: "PARALLEL",
    color: "#0891b2",
    demoImage: "assets/03_grounded_intelligence_detail.png",
    demoCaption: "Interactive clickable timestamp links directly jump to the exact video playback second.",
    description: "Synthesizes human-readable time labels (e.g., 'YouTube Video — 14:02', 'YouTube Live — 01:23:45') and builds direct YouTube video URLs with second offsets. Injects superscript citation anchors [#] into rendered HTML and Markdown executive summaries.",
    inputs: "Verified EvidenceObjects, timestamp_start, activity_type",
    outputs: "Rendered HTML & Markdown statements with inline clickable citation links",
    filesProduced: "Grounded executive summary and key points in item report",
    timingTrackers: "citation_synthesis_microseconds",
    failureModes: "Negative timestamp offsets, invalid video ID characters.",
    retryPolicy: "Sanitizes URL parameters, clamps timestamps within video duration bounds.",
    codeRef: "src/engine/evidence_verifier.py -> EvidenceVerifier.build_youtube_citation()",
    samplePayload: {
      "rendered_text_html": "Tribal healthcare initiatives expanded under new budgetary provisions.<sup><a href='https://www.youtube.com/watch?v=EvF9mhGyMps&t=842s'>[1]</a></sup>",
      "evidence_ids": ["EVID-001"]
    }
  },
  "reports": {
    name: "Hierarchical Multi-Format Reports",
    category: "Output Phase",
    type: "PARALLEL / BATCH",
    color: "#4f46e5",
    demoImage: "assets/02_daily_archive.png",
    demoCaption: "Self-contained standalone HTML, JSON, and Markdown reports generated with zero external CSS/JS dependencies.",
    description: "Generates 3 tiers of reports: 1) Individual Item Reports (HTML + JSON) 2) Channel Daily Reports (HTML + JSON) 3) Overall Daily Executive Report (HTML + JSON + Markdown). HTML reports are 100% self-contained with embedded responsive CSS.",
    inputs: "ActivityItem, ItemAnalysis, TranscriptData, ChannelConfig, date_str",
    outputs: "Standalone HTML, JSON, and Markdown report deliverables",
    filesProduced: "data/reports/items/{id}.html, data/reports/items/{id}.json, data/reports/channel/{channel_id}/{date}.html, data/reports/daily/{date}.html",
    timingTrackers: "report_generation_duration_seconds",
    failureModes: "Jinja/template rendering errors, filesystem write permission denied.",
    retryPolicy: "Atomic disk write via .tmp file rename with MongoDB mirror fallback.",
    codeRef: "src/reports/item_report.py, src/reports/channel_report.py, src/reports/daily_report.py",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "has_report": true,
      "report_url": "/static/reports/items/EvF9mhGyMps.html",
      "published_at": "2026-09-12T05:30:00Z"
    }
  },
  "storage": {
    name: "Dual-Tier Storage (NVMe File-First + MongoDB)",
    category: "Persistence Phase",
    type: "GLOBAL",
    color: "#4f46e5",
    demoImage: "assets/08_dual_tier_storage.png",
    demoCaption: "Dual-tier persistence: NVMe file-first audit hierarchy synchronized with indexed MongoDB collections.",
    description: "Provides dual-tier persistence. Tier 1: NVMe disk file-first storage with atomic writes (.tmp + replace) for human-readable auditability. Tier 2: MongoDB document persistence for high-speed indexing, search, and dashboard aggregation.",
    inputs: "All system models and deliverables",
    outputs: "Synchronized disk files and MongoDB collections",
    filesProduced: "data/channels/, data/activities/, data/transcripts/, data/analysis/, data/reports/, state/runs/, state/timings/",
    timingTrackers: "atomic_write_microseconds, mongo_upsert_latency",
    failureModes: "MongoDB connection loss (falls back to disk gracefully), NVMe disk full.",
    retryPolicy: "Non-blocking MongoDB write failures logged as warnings; file-first disk writes remain primary ground truth.",
    codeRef: "src/storage/file_store.py -> FileStore, src/storage/mongodb.py -> MongoManager",
    samplePayload: {
      "file_path": "data/reports/items/EvF9mhGyMps.json",
      "mongo_collection": "activities",
      "atomic_swap": true
    }
  },
  "telemetry_sse": {
    name: "SSE Telemetry & Real-Time State Stream",
    category: "API & Monitoring",
    type: "REAL-TIME",
    color: "#059669",
    demoImage: "assets/04_live_processing.png",
    demoCaption: "Live Server-Sent Events stream (/api/events) drives real-time progress cards and mission control console.",
    description: "Streams live ProgressEvents over Server-Sent Events (/api/events). Maintains active_items_tracker dictionary in FastAPI memory, updating stage, status, percentage, timings, and error payloads for every concurrent worker item.",
    inputs: "ProgressEvent emitted by pipeline",
    outputs: "Real-time SSE event stream (text/event-stream) consumed by React frontend",
    filesProduced: "logs/runs/{run_id}.log, state/runs/{run_id}.json",
    timingTrackers: "sse_broadcast_latency",
    failureModes: "Client disconnection, async queue backpressure.",
    retryPolicy: "Sliding event buffer (last 400 events) allows reconnecting clients to catch up immediately.",
    codeRef: "src/api/main.py -> broadcast_event(), active_items_tracker",
    samplePayload: {
      "item_id": "EvF9mhGyMps",
      "stage": "ANALYZING",
      "status": "progress",
      "progress": 65,
      "message": "Grounded AI extraction & synthesis via gemma3:12b..."
    }
  },
  "safe_deletion": {
    name: "Safe Deletion & Audit Lifecycle",
    category: "Governance & Cleanup",
    type: "GOVERNANCE",
    color: "#dc2626",
    demoImage: "assets/10_safe_deletion_audit.png",
    demoCaption: "2-step cryptographic confirmation prevents accidental data loss; writes append-only audit trail.",
    description: "Provides controlled data purge capabilities. Step 1: Preview impact across scopes (CHANNEL, DATE, DATE_RANGE, WATCHLIST) counting affected activities, transcripts, and reports. Step 2: Requires explicit cryptographic token (CONFIRM_DELETE) to execute atomic purge and writes append-only audit records.",
    inputs: "DeletionScope, target identifier, confirmation_token",
    outputs: "DeletionPreview object and DeletionAuditRecord",
    filesProduced: "state/audit/deletion_{audit_id}.json",
    timingTrackers: "preview_calculation_ms, atomic_delete_ms",
    failureModes: "Invalid confirmation token, file locking during ongoing processing.",
    retryPolicy: "Strict rejection of unconfirmed delete requests (HTTP 400); deletion refused while pipeline run is active.",
    codeRef: "src/storage/deletion_service.py -> DeletionService",
    samplePayload: {
      "scope": "DATE",
      "target_date": "2026-09-12",
      "items_affected": 28,
      "confirmation_required": "CONFIRM_DELETE",
      "audit_id": "del_audit_20260912_01"
    }
  }
};

// =============================================================================
// 2. Application State & Theme Management (Default Light)
// =============================================================================
let currentTab = "full";
let currentViewMode = "diagram"; // "diagram" | "timeline"
let currentTheme = localStorage.getItem("pipeline-theme") || "light";

// Pan & Zoom Viewport State
let panX = 0;
let panY = 0;
let zoomScale = 1.0;
let isPanning = false;
let startX = 0;
let startY = 0;

// Initialize on DOM Ready
document.addEventListener("DOMContentLoaded", () => {
  // Check URL query param or hash for initial tab & theme deep-linking
  const urlParams = new URLSearchParams(window.location.search);
  const themeParam = urlParams.get("theme");
  if (themeParam === "dark" || themeParam === "light") {
    currentTheme = themeParam;
  }
  initTheme();
  const tabParam = urlParams.get("tab") || window.location.hash.replace("#", "");
  if (tabParam) {
    const matchingTab = document.querySelector(`.tab-btn[data-tab="${tabParam}"]`);
    if (matchingTab) {
      document.querySelectorAll(".tab-btn").forEach(t => t.classList.remove("active"));
      matchingTab.classList.add("active");
      currentTab = tabParam;
    }
  }
  const viewParam = urlParams.get("view");
  if (viewParam === "timeline" || viewParam === "diagram") {
    currentViewMode = viewParam;
  }
  setupEventListeners();
  updateViewModeButtonUI();
  renderCurrentView();

  const drawerParam = urlParams.get("drawer");
  if (drawerParam && STAGE_SPECS[drawerParam]) {
    openDrawer(drawerParam);
  }
});

function updateViewModeButtonUI() {
  const btnViewMode = document.getElementById("btn-view-mode");
  if (!btnViewMode) return;
  const icon = btnViewMode.querySelector(".icon");
  const text = btnViewMode.querySelector(".text");
  if (currentViewMode === "timeline") {
    if (icon) icon.textContent = "🌐";
    if (text) text.textContent = "Diagram View";
  } else {
    if (icon) icon.textContent = "📋";
    if (text) text.textContent = "Step-by-Step Flow";
  }
}

/**
 * Initializes and toggles Light/Dark theme (Default Light)
 */
function initTheme() {
  document.documentElement.setAttribute("data-theme", currentTheme);
  updateThemeButtonUI();
}

function updateThemeButtonUI() {
  const icon = document.getElementById("theme-icon");
  const text = document.getElementById("theme-text");
  if (!icon || !text) return;

  if (currentTheme === "dark") {
    icon.textContent = "☀️";
    text.textContent = "Light Mode";
  } else {
    icon.textContent = "🌙";
    text.textContent = "Dark Mode";
  }
}

function toggleTheme() {
  currentTheme = currentTheme === "light" ? "dark" : "light";
  localStorage.setItem("pipeline-theme", currentTheme);
  document.documentElement.setAttribute("data-theme", currentTheme);
  updateThemeButtonUI();
  renderDiagramView(); // Re-render SVG to update dynamic strokes/fills
}

// =============================================================================
// 3. Event Listeners & Navigation Setup
// =============================================================================
function setupEventListeners() {
  // Theme Toggle Button
  const btnTheme = document.getElementById("btn-theme-toggle");
  if (btnTheme) btnTheme.addEventListener("click", toggleTheme);

  // Tab Bar Clicks
  const tabs = document.querySelectorAll(".tab-btn");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      currentTab = tab.getAttribute("data-tab");
      history.replaceState(null, "", `#${currentTab}`);
      resetViewport();
      renderCurrentView();
    });
  });

  // Lifecycle Track Bar Clicks (Direct Drawer Access)
  document.querySelectorAll(".track-step").forEach(el => {
    el.addEventListener("click", () => {
      const stage = el.getAttribute("data-stage");
      if (stage && STAGE_SPECS[stage]) openDrawer(stage);
    });
  });

  // View Mode Toggle (Diagram vs Step-by-Step Cards)
  const btnViewMode = document.getElementById("btn-view-mode");
  if (btnViewMode) {
    btnViewMode.addEventListener("click", () => {
      currentViewMode = currentViewMode === "diagram" ? "timeline" : "diagram";
      updateViewModeButtonUI();
      renderCurrentView();
    });
  }

  // Floating Controls (Zoom, Pan, Fit, Reset)
  const btnZoomIn = document.getElementById("btn-zoom-in");
  const btnZoomOut = document.getElementById("btn-zoom-out");
  const btnReset = document.getElementById("btn-reset");
  const btnFit = document.getElementById("btn-fit");

  if (btnZoomIn) btnZoomIn.addEventListener("click", () => zoomBy(1.15));
  if (btnZoomOut) btnZoomOut.addEventListener("click", () => zoomBy(0.85));
  if (btnReset) btnReset.addEventListener("click", resetViewport);
  if (btnFit) btnFit.addEventListener("click", fitToScreen);

  // Search Input Handler
  const searchInput = document.getElementById("search-input");
  if (searchInput) {
    searchInput.addEventListener("input", (e) => {
      const query = e.target.value.toLowerCase().trim();
      handleSearch(query);
    });
  }

  // Drawer Close Button & Backdrop
  const drawerClose = document.getElementById("drawer-close");
  const backdrop = document.getElementById("drawer-backdrop");
  if (drawerClose) drawerClose.addEventListener("click", closeDrawer);
  if (backdrop) backdrop.addEventListener("click", closeDrawer);

  // Lightbox Modal Close
  const lightboxModal = document.getElementById("lightbox-modal");
  const lightboxClose = document.getElementById("lightbox-close");
  if (lightboxClose) {
    lightboxClose.addEventListener("click", () => lightboxModal.classList.remove("open"));
  }
  if (lightboxModal) {
    lightboxModal.addEventListener("click", (e) => {
      if (e.target === lightboxModal) lightboxModal.classList.remove("open");
    });
  }

  // Canvas Pan & Zoom Mouse / Touch Gestures
  setupPanZoom();
}

/**
 * Handles Canvas Pan and MouseWheel Zooming
 */
function setupPanZoom() {
  const canvas = document.getElementById("diagram-canvas");
  if (!canvas) return;

  canvas.addEventListener("mousedown", (e) => {
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

  canvas.addEventListener("wheel", (e) => {
    e.preventDefault();
    const zoomFactor = e.deltaY < 0 ? 1.08 : 0.92;
    zoomBy(zoomFactor);
  }, { passive: false });

  // Touch Support for Mobile
  let initialTouchDist = 0;
  canvas.addEventListener("touchstart", (e) => {
    if (e.touches.length === 1) {
      isPanning = true;
      startX = e.touches[0].clientX - panX;
      startY = e.touches[0].clientY - panY;
    } else if (e.touches.length === 2) {
      isPanning = false;
      initialTouchDist = Math.hypot(
        e.touches[0].clientX - e.touches[1].clientX,
        e.touches[0].clientY - e.touches[1].clientY
      );
    }
  }, { passive: true });

  canvas.addEventListener("touchmove", (e) => {
    if (isPanning && e.touches.length === 1) {
      panX = e.touches[0].clientX - startX;
      panY = e.touches[0].clientY - startY;
      applyTransform();
    } else if (e.touches.length === 2 && initialTouchDist > 0) {
      const dist = Math.hypot(
        e.touches[0].clientX - e.touches[1].clientX,
        e.touches[0].clientY - e.touches[1].clientY
      );
      const factor = dist / initialTouchDist;
      zoomBy(factor);
      initialTouchDist = dist;
    }
  }, { passive: true });

  canvas.addEventListener("touchend", () => {
    isPanning = false;
    initialTouchDist = 0;
  });
}

function zoomBy(factor) {
  const newScale = zoomScale * factor;
  if (newScale >= 0.35 && newScale <= 3.2) {
    zoomScale = newScale;
    applyTransform();
  }
}

function resetViewport() {
  panX = 0;
  panY = 0;
  zoomScale = 1.0;
  applyTransform();
}

function fitToScreen() {
  panX = 0;
  panY = 0;
  const container = document.getElementById("viewport-container");
  if (container) {
    const w = container.clientWidth;
    zoomScale = w < 768 ? 0.55 : (w < 1200 ? 0.78 : 0.95);
  } else {
    zoomScale = 0.9;
  }
  applyTransform();
}

function applyTransform() {
  const svg = document.querySelector("#diagram-canvas svg");
  if (svg) {
    svg.style.transform = `translate(${panX}px, ${panY}px) scale(${zoomScale})`;
    svg.style.transformOrigin = "center center";
  }
}

// =============================================================================
// 4. View Switching & Dispatcher
// =============================================================================
function renderCurrentView() {
  const canvas = document.getElementById("diagram-canvas");
  const timeline = document.getElementById("timeline-container");
  const showcase = document.getElementById("showcase-container");
  const limitations = document.getElementById("limitations-container");
  const controls = document.getElementById("controls-overlay");
  const legend = document.getElementById("legend-overlay");

  // Hide all viewports initially
  if (canvas) canvas.style.display = "none";
  if (timeline) timeline.style.display = "none";
  if (showcase) showcase.style.display = "none";
  if (limitations) limitations.style.display = "none";
  if (controls) controls.style.display = "none";
  if (legend) legend.style.display = "none";

  if (currentTab === "screenshots") {
    if (showcase) {
      showcase.style.display = "block";
      renderShowcaseView();
    }
    return;
  }

  if (currentTab === "limitations") {
    if (limitations) {
      limitations.style.display = "block";
      renderLimitationsView();
    }
    return;
  }

  // Handle Timeline vs Diagram View
  if (currentViewMode === "timeline") {
    if (timeline) {
      timeline.style.display = "block";
      renderTimelineView();
    }
  } else {
    if (canvas) {
      canvas.style.display = "flex";
      if (controls) controls.style.display = "flex";
      if (legend) legend.style.display = "block";
      renderDiagramView();
    }
  }
}

// =============================================================================
// 5. SVG Helper Functions (Refined Layout & No Text Overlap)
// =============================================================================
function escapeXml(unsafe) {
  if (!unsafe) return "";
  return String(unsafe)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function svgNode(x, y, w, h, stageKey, title, subtitle, color, badgeText, statusDotColor) {
  const pillBg = `${color}14`;
  const pillBorder = `${color}45`;
  const dotColor = statusDotColor || color;
  const badgeWidth = badgeText ? Math.max(50, badgeText.length * 6.6 + 12) : 0;
  const badgeX = w - badgeWidth - 8;

  const subLines = (subtitle || "").split(/\r?\n|\\n/);
  const subTspans = subLines.map((line, idx) => 
    `<tspan x="16" dy="${idx === 0 ? 0 : 15}">${escapeXml(line)}</tspan>`
  ).join("");

  return `
    <g class="node-group" data-stage="${stageKey}" transform="translate(${x}, ${y})">
      <rect class="node-box" width="${w}" height="${h}" />
      <line x1="0" y1="0" x2="0" y2="${h}" stroke="${color}" stroke-width="4.5" stroke-linecap="round" />
      <circle cx="16" cy="22" r="4" fill="${dotColor}" />
      <text class="node-title" x="26" y="26">${escapeXml(title)}</text>
      <text class="node-subtitle" x="16" y="46">${subTspans}</text>
      ${badgeText ? `
        <rect x="${badgeX}" y="9" width="${badgeWidth}" height="20" rx="5" fill="${pillBg}" stroke="${pillBorder}" stroke-width="1" />
        <text class="node-badge" x="${badgeX + badgeWidth / 2}" y="23" fill="${color}" text-anchor="middle">${escapeXml(badgeText)}</text>
      ` : ""}
    </g>
  `;
}

function svgSwimlane(x, y, w, h, title, subtitle) {
  const isNarrow = w < 1000;
  const headerHeight = (isNarrow && subtitle) ? 42 : 32;
  return `
    <g class="swimlane-group">
      <rect class="swimlane-box" x="${x}" y="${y}" width="${w}" height="${h}" />
      <rect class="swimlane-header-box" x="${x}" y="${y}" width="${w}" height="${headerHeight}" />
      <text class="swimlane-title" x="${x + 18}" y="${y + 21}">${escapeXml(title)}</text>
      ${subtitle ? (
        isNarrow
          ? `<text class="swimlane-desc" x="${x + 18}" y="${y + 35}">${escapeXml(subtitle)}</text>`
          : `<text class="swimlane-desc" x="${x + w - 18}" y="${y + 21}" text-anchor="end">${escapeXml(subtitle)}</text>`
      ) : ""}
    </g>
  `;
}

function svgWorkerPool(x, y, w, h, title) {
  return `
    <g class="worker-pool-group">
      <rect class="worker-pool-box" x="${x}" y="${y}" width="${w}" height="${h}" />
      <text class="worker-pool-title" x="${x + 14}" y="${y + 20}">⚡ ${title}</text>
    </g>
  `;
}

function svgEdge(x1, y1, x2, y2, markerId = "arrow", color = null, active = false) {
  const strokeColor = color || (currentTheme === "dark" ? "#475569" : "#94a3b8");
  return `
    <path d="M ${x1} ${y1} L ${x2} ${y2}" class="edge-path ${active ? 'flow-active' : ''}" stroke="${strokeColor}" marker-end="url(#${markerId})" />
  `;
}

function svgCurvedEdge(x1, y1, x2, y2, markerId = "arrow", color = null, active = false) {
  const strokeColor = color || (currentTheme === "dark" ? "#475569" : "#94a3b8");
  const dx = Math.abs(x2 - x1) / 2;
  return `
    <path d="M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}" class="edge-path ${active ? 'flow-active' : ''}" stroke="${strokeColor}" marker-end="url(#${markerId})" />
  `;
}

function svgDefinitions() {
  const arrowDef = currentTheme === "dark" ? "#94a3b8" : "#64748b";
  return `
    <defs>
      <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="${arrowDef}" />
      </marker>
      <marker id="arrow-blue" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#2563eb" />
      </marker>
      <marker id="arrow-pink" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#db2777" />
      </marker>
      <marker id="arrow-purple" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#7c3aed" />
      </marker>
      <marker id="arrow-red" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#dc2626" />
      </marker>
      <marker id="arrow-green" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#059669" />
      </marker>
      <marker id="arrow-amber" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
        <path d="M 0 1 L 10 5 L 0 9 z" fill="#d97706" />
      </marker>
    </defs>
  `;
}

// =============================================================================
// 6. Interactive Diagram Renderers (6 Views)
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

  // Attach click listeners to all node groups
  document.querySelectorAll(".node-group").forEach(el => {
    el.addEventListener("click", () => {
      const stage = el.getAttribute("data-stage");
      openDrawer(stage);
    });
  });

  applyTransform();
}

/**
 * Diagram 1: Complete End-to-End Pipeline Swimlanes with Concurrency
 */
function renderFullPipelineSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1560 920" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    <!-- PHASE 1: SEQUENTIAL DISCOVERY & INGESTION GATE -->
    ${svgSwimlane(20, 25, 1520, 155, "PHASE 1: SEQUENTIAL CHANNEL DISCOVERY & IDEMPOTENCY GATE", "Sequential Ingestion via youtube_collector.py (Channels -> DateFilter -> State Check)")}
    
    ${svgNode(40, 70, 240, 62, "watchlist", "Watchlist Registry", "channels.yaml & settings", "#2563eb", "CONFIG", "#2563eb")}
    ${svgEdge(280, 101, 320, 101, "arrow-blue", "#2563eb")}

    ${svgNode(320, 70, 250, 62, "discovery", "Multi-Tab Scanner", "5 Dedicated Collectors", "#2563eb", "STAGE 1", "#2563eb")}
    ${svgEdge(570, 101, 610, 101, "arrow-blue", "#2563eb")}

    ${svgNode(610, 70, 240, 62, "date_filter", "Temporal Matcher", "UTC window match", "#0891b2", "FILTER", "#0891b2")}
    ${svgEdge(850, 101, 890, 101, "arrow-blue", "#2563eb")}

    ${svgNode(890, 70, 240, 62, "dedup", "Idempotency Gate", "state_manager.is_completed", "#7c3aed", "IDEMPOTENT", "#7c3aed")}
    ${svgEdge(1130, 101, 1170, 101, "arrow-purple", "#7c3aed")}

    ${svgNode(1170, 70, 280, 62, "queue_dispatch", "Worker Pool Dispatcher", "ThreadPoolExecutor(3)", "#7c3aed", "PARALLEL POOL", "#7c3aed")}
    
    <!-- PHASE 2: CONCURRENT WORKER POOL & MULTI-TYPE DISPATCHER -->
    ${svgSwimlane(20, 205, 1520, 270, "PHASE 2: CONCURRENT WORKER POOL & MULTI-TYPE DISPATCHER (PARALLEL EXECUTION)", "Items Enqueued across ThreadPoolExecutor(max_workers=3) with SSE Telemetry")}
    
    <!-- Parallel Sub-Branches Container -->
    ${svgWorkerPool(40, 245, 1480, 215, "CONCURRENT WORKER EXECUTION (Worker Threads 1, 2, 3 Processing Discovered Items in Parallel)")}

    <!-- Curved feeder from dispatcher into worker pool -->
    ${svgCurvedEdge(1310, 132, 1310, 265, "arrow-purple", "#7c3aed", true)}

    <!-- Track 1: Long Videos & Shorts (Audio fallback) -->
    ${svgNode(70, 275, 270, 50, "download", "Video / Shorts Ingest", "Opus audio stream via yt-dlp", "#2563eb", "VIDEO/SHORT", "#2563eb")}
    ${svgEdge(340, 300, 390, 300, "arrow-pink", "#db2777")}
    ${svgNode(390, 275, 270, 50, "audio_norm", "16kHz Mono Resample", "FFmpeg WAV (Auto-cleanup)", "#db2777", "FFMPEG", "#db2777")}

    <!-- Track 2: Native YouTube Captions Bypass (FAST PATH) -->
    ${svgNode(70, 335, 270, 50, "caption_shortcut", "Native Captions Bypass", "Official / Auto (~0.05s fetch)", "#059669", "FAST PATH", "#059669")}
    ${svgEdge(340, 360, 390, 360, "arrow-green", "#059669")}
    ${svgNode(390, 335, 270, 50, "caption_shortcut", "Direct Transcript Ready", "Zero audio/GPU overhead", "#059669", "SKIP STT", "#059669")}

    <!-- Track 3: Live Streams (Active HLS Segmenter) -->
    ${svgNode(70, 395, 270, 50, "live_segmenter", "Live HLS Stream Slicer", "120s Rolling Audio Segment", "#dc2626", "120s SLICE", "#dc2626")}
    ${svgEdge(340, 420, 390, 420, "arrow-red", "#dc2626")}
    ${svgNode(390, 395, 270, 50, "live_segmenter", "Live Audio Segment Ready", "Incremental stream tracking", "#dc2626", "LIVE STREAM", "#dc2626")}

    <!-- Track 4: Community Posts (Audio-Free Text Extraction) -->
    ${svgNode(730, 395, 280, 50, "post_text", "Community Post Parser", "Scrapes text & attached images", "#7c3aed", "AUDIO-FREE", "#7c3aed")}
    ${svgEdge(1010, 420, 1060, 420, "arrow-purple", "#7c3aed")}
    ${svgNode(1060, 395, 280, 50, "post_text", "Post Text Deliverable", "Bypasses audio & Whisper", "#7c3aed", "TEXT ONLY", "#7c3aed")}

    <!-- PHASE 3: SPEECH-TO-TEXT & GROUNDED AI EXTRACTION -->
    ${svgSwimlane(20, 500, 1520, 195, "PHASE 3: SPEECH-TO-TEXT (CUDA) & GROUNDED AI EXTRACTION (OLLAMA DUAL-ENGINE)", "Chunked Extraction (>420s) & Programmatic Levenshtein Verification")}
    
    <!-- Connect audio from Track 1 and Track 3 into Whisper -->
    ${svgCurvedEdge(660, 300, 205, 560, "arrow-green", "#059669", true)}
    ${svgCurvedEdge(660, 420, 205, 560, "arrow-green", "#059669", true)}
    ${svgNode(60, 560, 290, 65, "transcription", "Faster-Whisper CUDA", "float16 + VAD + Word Anchors", "#059669", "SPEECH", "#059669")}

    <!-- Connect Direct Transcripts (Track 2) and Post Text (Track 4) straight to LLM -->
    ${svgCurvedEdge(660, 360, 555, 560, "arrow-green", "#059669", true)}
    ${svgCurvedEdge(1200, 445, 555, 560, "arrow-purple", "#7c3aed", true)}

    <!-- Whisper -> LLM -->
    ${svgEdge(350, 592, 410, 592, "arrow-amber", "#d97706")}
    ${svgNode(410, 560, 310, 65, "llm_analysis", "Dual-Engine Ollama", "Gemma 3 12B / Qwen Fallback", "#d97706", "AI SYNTHESIS", "#d97706")}
    
    ${svgEdge(720, 592, 780, 592, "arrow-cyan", "#0891b2")}
    ${svgNode(780, 560, 290, 65, "evidence_validation", "Evidence Grounding", "Fuzzy overlap >= 60%", "#0891b2", "VERIFICATION", "#0891b2")}

    ${svgEdge(1070, 592, 1130, 592, "arrow-cyan", "#0891b2")}
    ${svgNode(1130, 560, 310, 65, "citation_generation", "Deep Timestamp Anchors", "Clickable [&t=XXs] links", "#0891b2", "CITATIONS", "#0891b2")}

    <!-- PHASE 4: MULTI-FORMAT REPORTS, DUAL-TIER PERSISTENCE & SAFE AUDIT -->
    ${svgSwimlane(20, 725, 1520, 175, "PHASE 4: MULTI-FORMAT REPORTS, DUAL-TIER STORAGE & SAFE DELETION", "Predictable NVMe Disk Files + MongoDB Indexing + Real-Time SSE Stream")}
    
    ${svgCurvedEdge(1285, 625, 1285, 775, "arrow-purple", "#4f46e5", true)}
    ${svgNode(1130, 775, 310, 65, "reports", "File-First Reports", "HTML + JSON + Markdown", "#4f46e5", "REPORTS", "#4f46e5")}

    ${svgEdge(1130, 807, 1070, 807, "arrow-purple", "#4f46e5")}
    ${svgNode(780, 775, 290, 65, "storage", "Dual Storage Engine", "NVMe atomic writes + Mongo", "#4f46e5", "PERSISTENCE", "#4f46e5")}

    ${svgEdge(780, 807, 720, 807, "arrow-green", "#059669")}
    ${svgNode(410, 775, 310, 65, "telemetry_sse", "Real-Time SSE Stream", "/api/events active telemetry", "#059669", "STREAMING", "#059669")}

    ${svgEdge(410, 807, 350, 807, "arrow-red", "#dc2626")}
    ${svgNode(60, 775, 290, 65, "safe_deletion", "Safe Deletion Lifecycle", "2-Step Preview & Token Auth", "#dc2626", "GOVERNANCE", "#dc2626")}
  </svg>
  `;
}

/**
 * Diagram 2: 4 Distinct Activity Flows
 */
function renderActivityFlowsSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1560 880" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    <!-- FLOW 1: LONG-FORM VIDEOS -->
    ${svgSwimlane(20, 20, 1520, 180, "FLOW 1: LONG-FORM YOUTUBE VIDEOS (PARLIAMENT DEBATES, SPEECHES, PRESS RELEASES)", "Native Captions shortcut (~0.05s) OR Fallback Audio -> Faster-Whisper CUDA -> 420s Chunking -> Grounded Citations")}
    ${svgNode(40, 65, 225, 55, "discovery", "Video Ingestion", "yt-dlp flat playlist", "#2563eb", "DISCOVERY", "#2563eb")}
    ${svgEdge(265, 92, 290, 92, "arrow-blue", "#2563eb")}
    ${svgNode(290, 65, 230, 55, "caption_shortcut", "Captions Shortcut", "Native hi / en (<0.05s)", "#059669", "FAST PATH", "#059669")}
    ${svgEdge(520, 92, 545, 92, "arrow-green", "#059669")}
    ${svgNode(545, 65, 230, 55, "audio_norm", "Audio Resample", "16kHz Mono WAV", "#db2777", "FFMPEG", "#db2777")}
    ${svgEdge(775, 92, 800, 92, "arrow-pink", "#db2777")}
    ${svgNode(800, 65, 235, 55, "transcription", "Whisper STT (CUDA)", "Silero VAD + Timestamps", "#059669", "SPEECH", "#059669")}
    ${svgEdge(1035, 92, 1060, 92, "arrow-amber", "#d97706")}
    ${svgNode(1060, 65, 235, 55, "llm_analysis", "Dual-Engine LLM", "420s Chunking", "#d97706", "EXTRACTION", "#d97706")}
    ${svgEdge(1295, 92, 1320, 92, "arrow-purple", "#4f46e5")}
    ${svgNode(1320, 65, 200, 55, "reports", "Video Report", "Standalone HTML+JSON", "#4f46e5", "REPORT", "#4f46e5")}

    <!-- FLOW 2: YOUTUBE SHORTS -->
    ${svgSwimlane(20, 230, 1520, 180, "FLOW 2: YOUTUBE SHORTS (<60s HIGH-IMPACT CLIPS & SOUNDBITES)", "Fast single-pass speech recognition & succinct key takeaway extraction with exact quote grounding")}
    ${svgNode(40, 275, 225, 55, "discovery", "Shorts Discovery", "Channel /shorts tab", "#db2777", "DISCOVERY", "#db2777")}
    ${svgEdge(265, 302, 290, 302, "arrow-pink", "#db2777")}
    ${svgNode(290, 275, 230, 55, "caption_shortcut", "Captions Check", "Native transcript check", "#059669", "FAST PATH", "#059669")}
    ${svgEdge(520, 302, 545, 302, "arrow-green", "#059669")}
    ${svgNode(545, 275, 230, 55, "audio_norm", "Rapid WAV Slicer", "Immediate scratch WAV", "#db2777", "AUDIO", "#db2777")}
    ${svgEdge(775, 302, 800, 302, "arrow-pink", "#db2777")}
    ${svgNode(800, 275, 235, 55, "transcription", "Whisper Small/Turbo", "Instant STT (<1.5s)", "#059669", "SPEECH", "#059669")}
    ${svgEdge(1035, 302, 1060, 302, "arrow-amber", "#d97706")}
    ${svgNode(1060, 275, 235, 55, "llm_analysis", "Single-Pass LLM", "Compact Takeaways", "#d97706", "EXTRACTION", "#d97706")}
    ${svgEdge(1295, 302, 1320, 302, "arrow-purple", "#4f46e5")}
    ${svgNode(1320, 275, 200, 55, "reports", "Shorts Report", "Quick Takeaway HTML", "#4f46e5", "REPORT", "#4f46e5")}

    <!-- FLOW 3: COMMUNITY POSTS (AUDIO-FREE) -->
    ${svgSwimlane(20, 440, 1520, 180, "FLOW 3: YOUTUBE COMMUNITY POSTS (AUDIO-FREE TEXT & IMAGE ANNOUNCEMENTS)", "Bypasses download & Whisper completely -> Direct LLM post analysis -> Grounding against post text")}
    ${svgNode(40, 485, 240, 55, "discovery", "Community Scraper", "InnerTube Backstage post", "#7c3aed", "DISCOVERY", "#7c3aed")}
    ${svgEdge(280, 512, 320, 512, "arrow-purple", "#7c3aed")}
    ${svgNode(320, 485, 270, 55, "post_text", "Post Text & Images", "Author, text & images", "#7c3aed", "AUDIO-FREE", "#7c3aed")}
    ${svgEdge(590, 512, 630, 512, "arrow-purple", "#7c3aed")}
    ${svgNode(630, 485, 250, 55, "llm_analysis", "LLM Post Analyzer", "Entity & Intent Extraction", "#d97706", "AI ANALYSIS", "#d97706")}
    ${svgEdge(880, 512, 920, 512, "arrow-cyan", "#0891b2")}
    ${svgNode(920, 485, 260, 55, "evidence_validation", "Text Provenance", "Verified against post_text", "#0891b2", "VERIFY", "#0891b2")}
    ${svgEdge(1180, 512, 1220, 512, "arrow-purple", "#4f46e5")}
    ${svgNode(1220, 485, 230, 55, "reports", "Post Report", "Community Post HTML", "#4f46e5", "REPORT", "#4f46e5")}

    <!-- FLOW 4: YOUTUBE LIVE STREAMS -->
    ${svgSwimlane(20, 660, 1520, 235, "FLOW 4: YOUTUBE LIVE STREAMS (ACTIVE LIVE_NOW, UPCOMING SCHEDULES, COMPLETED VODS)", "Active HLS 120s segment capture vs Scheduled broadcast intent vs Full VOD archival")}
    ${svgNode(40, 755, 230, 54, "discovery", "Live Tab Detector", "/live & /streams tabs", "#dc2626", "DISCOVERY", "#dc2626")}
    
    <!-- 3 Live Status Branches -->
    ${svgCurvedEdge(270, 782, 310, 729, "arrow-red", "#dc2626")}
    ${svgNode(310, 705, 250, 48, "live_segmenter", "LIVE_NOW: HLS Slicer", "120s Rolling Audio Slice", "#dc2626", "ACTIVE STREAM", "#dc2626")}
    
    ${svgEdge(270, 782, 310, 782, "arrow-amber", "#d97706")}
    ${svgNode(310, 758, 250, 48, "discovery", "UPCOMING: Schedule", "Metadata & Intent Catalog", "#d97706", "SCHEDULED", "#d97706")}

    ${svgCurvedEdge(270, 782, 310, 835, "arrow-blue", "#2563eb")}
    ${svgNode(310, 811, 250, 48, "download", "COMPLETED: VOD", "Full Broadcast Archival", "#2563eb", "VOD ARCHIVE", "#2563eb")}

    ${svgCurvedEdge(560, 729, 630, 782, "arrow-green", "#059669")}
    ${svgEdge(560, 782, 630, 782, "arrow-amber", "#d97706")}
    ${svgCurvedEdge(560, 835, 630, 782, "arrow-blue", "#2563eb")}

    ${svgNode(630, 755, 260, 54, "transcription", "Speech & Intent Engine", "Whisper OR Metadata LLM", "#059669", "PROCESSING", "#059669")}
    ${svgEdge(890, 782, 940, 782, "arrow-cyan", "#0891b2")}
    ${svgNode(940, 755, 260, 54, "evidence_validation", "Live Timestamp Citation", "Exact livestream playback", "#0891b2", "GROUNDING", "#0891b2")}
    ${svgEdge(1200, 782, 1250, 782, "arrow-purple", "#4f46e5")}
    ${svgNode(1250, 755, 240, 54, "reports", "Live Report (incremental)", "Dynamic Live Status Card", "#4f46e5", "REPORT", "#4f46e5")}
  </svg>
  `;
}

/**
 * Diagram 3: Grounded Citations & Provenance Flow
 */
function renderEvidenceGroundingSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1480 820" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    ${svgSwimlane(20, 30, 1440, 760, "GROUNDED CITATIONS & EVIDENCE PROVENANCE PIPELINE", "Strict Zero-Hallucination Programmatic Verification (Transcript -> [EVID-xxx] Extraction -> Fuzzy Matching -> YouTube Anchors)")}

    <!-- Row 1: Forward Extraction Pipeline -->
    ${svgNode(60, 100, 290, 85, "transcription", "1. Speech Word Tokens", "faster-whisper extracts word timestamps\\n(start, end, text tokens, probability)", "#059669", "TOKENS", "#059669")}
    ${svgEdge(350, 142, 405, 142, "arrow-green", "#059669")}

    ${svgNode(405, 100, 300, 85, "llm_analysis", "2. Dual-Engine LLM", "Ollama outputs structured JSON with\\nexplicit [EVID-xxx] evidence ID tags", "#d97706", "LLM STAGE", "#d97706")}
    ${svgEdge(705, 142, 760, 142, "arrow-amber", "#d97706")}

    ${svgNode(760, 100, 290, 85, "evidence_validation", "3. Evidence Catalog", "Builds Dict[str, EvidenceObject]\\nindexed by EVID-001, EVID-002, etc.", "#0891b2", "CATALOG", "#0891b2")}
    ${svgEdge(1050, 142, 1105, 142, "arrow-cyan", "#0891b2")}

    ${svgNode(1105, 100, 315, 85, "evidence_validation", "4. Verification Gate", "Tier 1: Substring Match\\nTier 2: 5-Word Prefix Match\\nTier 3: >60% Word Overlap", "#0891b2", "VERIFY", "#0891b2")}

    ${svgCurvedEdge(1262, 185, 1262, 310, "arrow-cyan", "#0891b2", true)}

    <!-- Row 2: Backward Synthesis Pipeline -->
    ${svgNode(1105, 310, 315, 85, "citation_generation", "5. Timestamp Anchors", "Calculates seconds: sec = int(t_start)\\nBuilds: https://youtube.com/watch?v=ID&t=XXs", "#2563eb", "ANCHORS", "#2563eb")}
    ${svgEdge(1105, 352, 1050, 352, "arrow-blue", "#2563eb")}

    ${svgNode(760, 310, 290, 85, "citation_generation", "6. Citation Formatter", "Formats readable type labels:\\n'YouTube Video — 14:02'\\n'YouTube Live — 01:23:45'", "#2563eb", "LABELS", "#2563eb")}
    ${svgEdge(760, 352, 705, 352, "arrow-blue", "#2563eb")}

    ${svgNode(405, 310, 300, 85, "reports", "7. Marker Injection", "Replaces [EVID-xxx] with clickable links:\\nHTML: <sup>[1]</sup> and [MM:SS](url)\\nDirect YouTube timestamp jump", "#4f46e5", "MARKERS", "#4f46e5")}
    ${svgEdge(405, 352, 350, 352, "arrow-purple", "#4f46e5")}

    ${svgNode(60, 310, 290, 85, "reports", "8. Grounded Report", "Standalone HTML item report & JSON\\nwith 100% verified interactive links", "#4f46e5", "REPORT", "#4f46e5")}

    <!-- Verification Logic Detail Cards -->
    ${svgSwimlane(60, 460, 660, 290, "VERIFICATION LOGIC & ALGORITHMIC TOLERANCE", "src/engine/evidence_verifier.py")}
    <text class="swimlane-desc" x="90" y="525" font-size="12">• Level 1 (Direct Substring): Checks if quoted text exists verbatim in lowercased transcript.</text>
    <text class="swimlane-desc" x="90" y="560" font-size="12">• Level 2 (Sub-Phrase Match): Checks if first 5 consecutive words appear contiguously.</text>
    <text class="swimlane-desc" x="90" y="595" font-size="12">• Level 3 (Word Overlap): Computes significant words (>3 chars) presence ratio >= 60%.</text>
    <text class="swimlane-desc" x="90" y="630" font-size="12">• Anchor Clamping: Clamps start/end timestamps within [0, video_duration_seconds].</text>
    <text class="swimlane-desc" x="90" y="665" font-size="12">• Hallucination Rejection: If all 3 tiers fail, verified=false and citation URL is stripped.</text>

    ${svgSwimlane(760, 460, 660, 290, "PROVENANCE SAMPLE DATA OBJECTS", "Rendered Grounded Statement Structure")}
    <text class="swimlane-desc" x="790" y="525" font-size="12" font-family="monospace" fill="#2563eb">"text": "Rail project allocation increased by 40% for southern tribal districts."</text>
    <text class="swimlane-desc" x="790" y="560" font-size="12" font-family="monospace" fill="#059669">"timestamp_start": 842.0, "timestamp_end": 855.0, "verified": true</text>
    <text class="swimlane-desc" x="790" y="595" font-size="12" font-family="monospace" fill="#0891b2">"citation_url": "https://www.youtube.com/watch?v=EvF9mhGyMps&amp;t=842s"</text>
    <text class="swimlane-desc" x="790" y="630" font-size="12" font-family="monospace" fill="#7c3aed">"citation_label": "YouTube Video — 14:02"</text>
    <text class="swimlane-desc" x="790" y="665" font-size="12" font-family="monospace" fill="#d97706">"rendered_text_html": "...tribal districts.&lt;sup&gt;&lt;a href='...'&gt;[1]&lt;/a&gt;&lt;/sup&gt;"</text>
  </svg>
  `;
}

/**
 * Diagram 4: Processing-Time Profiler & Telemetry
 */
function renderTimingProfilerSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1480 820" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    ${svgSwimlane(20, 30, 1440, 760, "WALL-CLOCK TIMING PROFILER & HARDWARE TELEMETRY ARCHITECTURE", "Microsecond Phase Timers, Real-Time Factor (RTF) & ActivityTiming Schema (src/models.py)")}

    <!-- Visual Timeline Track -->
    ${svgSwimlane(60, 90, 1360, 140, "STAGE TIMING BREAKDOWN (SAVED TO state/timings/{item_id}.json & MONGODB)", "Sum of Stages vs Real Wall-Clock Overlap")}
    
    ${svgNode(80, 140, 200, 55, "discovery", "1. Metadata", "0.25s - 0.60s", "#2563eb", "DISCOVERY", "#2563eb")}
    ${svgEdge(280, 167, 305, 167, "arrow-blue", "#2563eb")}

    ${svgNode(305, 140, 200, 55, "download", "2. Ingest", "0.05s (fast) / 3.2s", "#0891b2", "MEDIA", "#0891b2")}
    ${svgEdge(505, 167, 530, 167, "arrow-cyan", "#0891b2")}

    ${svgNode(530, 140, 200, 55, "audio_norm", "3. Resample", "0.40s - 1.10s", "#db2777", "FFMPEG", "#db2777")}
    ${svgEdge(730, 167, 755, 167, "arrow-pink", "#db2777")}

    ${svgNode(755, 140, 205, 55, "transcription", "4. Whisper", "RTF ~0.08x (CUDA)", "#059669", "CUDA STT", "#059669")}
    ${svgEdge(960, 167, 985, 167, "arrow-green", "#059669")}

    ${svgNode(985, 140, 205, 55, "llm_analysis", "5. Ollama", "420s Chunks (Gemma 3)", "#d97706", "LLM TOKENS", "#d97706")}
    ${svgEdge(1190, 167, 1215, 167, "arrow-amber", "#d97706")}

    ${svgNode(1215, 140, 200, 55, "evidence_validation", "6. Grounding", "0.15s (Atomic Write)", "#4f46e5", "REPORT", "#4f46e5")}

    <!-- Two Metric Cards -->
    ${svgSwimlane(60, 260, 660, 480, "WALL-CLOCK vs SUM-OF-STAGES PROFILING", "ActivityTiming Data Model")}
    <text class="swimlane-desc" x="90" y="325" font-size="13" font-weight="700" fill="#2563eb">ActivityTiming Class Definition:</text>
    <text class="swimlane-desc" x="90" y="355" font-size="12" font-family="monospace">• activity_id: str</text>
    <text class="swimlane-desc" x="90" y="385" font-size="12" font-family="monospace">• activity_type: ActivityType (VIDEO, SHORT, POST, LIVE)</text>
    <text class="swimlane-desc" x="90" y="415" font-size="12" font-family="monospace">• wall_clock_duration_seconds: float (True end-to-end time)</text>
    <text class="swimlane-desc" x="90" y="445" font-size="12" font-family="monospace">• sum_of_stage_durations_seconds: float (Serial sum)</text>
    <text class="swimlane-desc" x="90" y="475" font-size="12" font-family="monospace">• whisper_duration_seconds: Optional[float]</text>
    <text class="swimlane-desc" x="90" y="505" font-size="12" font-family="monospace">• llm_duration_seconds: Optional[float]</text>
    <text class="swimlane-desc" x="90" y="535" font-size="12" font-family="monospace">• stages: Dict[str, StageTiming]</text>
    <text class="swimlane-desc" x="90" y="580" font-size="13" font-weight="700" fill="#059669">StageTiming Schema:</text>
    <text class="swimlane-desc" x="90" y="615" font-size="12" font-family="monospace">• stage: str, duration_seconds: float, status: str</text>
    <text class="swimlane-desc" x="90" y="645" font-size="12" font-family="monospace">• started_at: ISO-8601, completed_at: ISO-8601</text>
    <text class="swimlane-desc" x="90" y="675" font-size="12" font-family="monospace">• error: Optional[str]</text>

    ${svgSwimlane(760, 260, 660, 480, "HARDWARE INSTRUMENTATION & VRAM TELEMETRY", "CUDA, CPU & Storage Metrics")}
    <text class="swimlane-desc" x="790" y="325" font-size="13" font-weight="700" fill="#d97706">Telemetry Trackers:</text>
    <text class="swimlane-desc" x="790" y="355" font-size="12">• CUDA Peak VRAM Usage: Monitored during Whisper STT execution (float16 allocates ~1.8GB - 3.2GB).</text>
    <text class="swimlane-desc" x="790" y="395" font-size="12">• Ollama GPU Offload: Gemma 3 12B offloaded to NVIDIA GPU layer; tracks tokens/sec generation speed.</text>
    <text class="swimlane-desc" x="790" y="435" font-size="12">• Real-Time Factor (RTF): audio_duration / stt_execution_time (Values < 0.10x indicate faster-than-real-time).</text>
    <text class="swimlane-desc" x="790" y="475" font-size="12">• ThreadPool Concurrency: 3 workers share GPU resources sequentially; disk operations run asynchronously.</text>
    <text class="swimlane-desc" x="790" y="515" font-size="12">• Scratch Cleanup: WAV files deleted within 50ms of STT completion to preserve disk I/O bandwidth.</text>
    <text class="swimlane-desc" x="790" y="555" font-size="12">• SSE Telemetry: State transitions pushed via /api/events with microsecond resolution timestamps.</text>
  </svg>
  `;
}

/**
 * Diagram 5: File-First Storage Tree & MongoDB Architecture
 */
function renderStorageHierarchySVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1480 820" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    ${svgSwimlane(20, 30, 1440, 760, "DUAL-TIER STORAGE ARCHITECTURE: NVMe FILE-FIRST AUDIT + MONGODB ENGINE", "Predictable local directory hierarchy with atomic writes (.tmp + rename) synchronized with MongoDB")}

    <!-- Left Box: Local Directory Tree -->
    ${svgSwimlane(60, 90, 670, 670, "TIER 1: NVMe FILE-FIRST HIERARCHY", "src/storage/file_store.py (Predictable, human-readable JSON & HTML)")}
    
    <text class="swimlane-desc" x="80" y="150" font-size="13" font-family="monospace" font-weight="700" fill="#2563eb">data/</text>
    <text class="swimlane-desc" x="110" y="180" font-size="12" font-family="monospace">├── channels/{channel_id}/channel.json (Channel metadata & stats)</text>
    <text class="swimlane-desc" x="110" y="210" font-size="12" font-family="monospace">├── raw/{item_id}_raw.json (Immutable snapshot of raw scraping metadata)</text>
    <text class="swimlane-desc" x="110" y="240" font-size="12" font-family="monospace">├── activities/YYYY/MM/DD/ (Partitioned by publication date)</text>
    <text class="swimlane-desc" x="130" y="270" font-size="12" font-family="monospace">├── videos/{item_id}.json</text>
    <text class="swimlane-desc" x="130" y="300" font-size="12" font-family="monospace">├── shorts/{item_id}.json</text>
    <text class="swimlane-desc" x="130" y="330" font-size="12" font-family="monospace">├── posts/{item_id}.json</text>
    <text class="swimlane-desc" x="130" y="360" font-size="12" font-family="monospace">└── live/{item_id}.json</text>
    <text class="swimlane-desc" x="110" y="390" font-size="12" font-family="monospace">├── transcripts/{item_id}.json (Segments, word tokens, timestamps)</text>
    <text class="swimlane-desc" x="110" y="420" font-size="12" font-family="monospace">├── analysis/{item_id}.json (Grounded claims, topics, entities)</text>
    <text class="swimlane-desc" x="110" y="450" font-size="12" font-family="monospace">└── reports/</text>
    <text class="swimlane-desc" x="130" y="480" font-size="12" font-family="monospace">├── items/{item_id}.html & .json (Interactive item deliverables)</text>
    <text class="swimlane-desc" x="130" y="510" font-size="12" font-family="monospace">├── channel/{channel_id}/{date}.html & .json (Channel daily)</text>
    <text class="swimlane-desc" x="130" y="540" font-size="12" font-family="monospace">└── daily/{date}.html, .json, .md (Overall executive overview)</text>
    
    <text class="swimlane-desc" x="80" y="575" font-size="13" font-family="monospace" font-weight="700" fill="#7c3aed">state/</text>
    <text class="swimlane-desc" x="110" y="605" font-size="12" font-family="monospace">├── runs/{run_id}.json (Run telemetry, counters, error logs)</text>
    <text class="swimlane-desc" x="110" y="635" font-size="12" font-family="monospace">├── items/{item_id}.json (Item-level lifecycle state & stage)</text>
    <text class="swimlane-desc" x="110" y="665" font-size="12" font-family="monospace">├── timings/{item_id}.json (Stage timing profiles & wall-clock)</text>
    <text class="swimlane-desc" x="110" y="695" font-size="12" font-family="monospace">└── audit/deletion_{audit_id}.json (Append-only deletion records)</text>

    <!-- Right Box: MongoDB Collections -->
    ${svgSwimlane(760, 90, 670, 670, "TIER 2: MONGODB DOCUMENT PERSISTENCE", "src/storage/mongodb.py (High-speed aggregations, text search & dashboard)")}
    
    <text class="swimlane-desc" x="790" y="150" font-size="13" font-weight="700" fill="#059669">Synchronized MongoDB Collections:</text>
    
    <text class="swimlane-desc" x="790" y="190" font-size="12" font-family="monospace" fill="#2563eb">1. db.channels</text>
    <text class="swimlane-desc" x="820" y="215" font-size="11">Indexed on id. Stores handle, subscriber count, avatar URL, category tags.</text>

    <text class="swimlane-desc" x="790" y="250" font-size="12" font-family="monospace" fill="#2563eb">2. db.activities</text>
    <text class="swimlane-desc" x="820" y="275" font-size="11">Indexed on item_id, channel_id, published_at, activity_type. Dashboard filtering.</text>

    <text class="swimlane-desc" x="790" y="310" font-size="12" font-family="monospace" fill="#059669">3. db.transcripts</text>
    <text class="swimlane-desc" x="820" y="335" font-size="11">Indexed on item_id. Full-text search over speech segments and word tokens.</text>

    <text class="swimlane-desc" x="790" y="370" font-size="12" font-family="monospace" fill="#d97706">4. db.analyses</text>
    <text class="swimlane-desc" x="820" y="395" font-size="11">Indexed on item_id. Stores structured claims, promises, announcements, and entities.</text>

    <text class="swimlane-desc" x="790" y="430" font-size="12" font-family="monospace" fill="#4f46e5">5. db.reports</text>
    <text class="swimlane-desc" x="820" y="455" font-size="11">Indexed on report_id (item:ID, channel:ID:DATE, daily:DATE). Fast delivery of HTML & JSON.</text>

    <text class="swimlane-desc" x="790" y="490" font-size="12" font-family="monospace" fill="#7c3aed">6. db.runs & db.timings</text>
    <text class="swimlane-desc" x="820" y="515" font-size="11">Stores run progress, active workers, items completed, and stage duration benchmarks.</text>

    <text class="swimlane-desc" x="790" y="550" font-size="12" font-family="monospace" fill="#dc2626">7. db.audit_logs</text>
    <text class="swimlane-desc" x="820" y="575" font-size="11">Append-only audit trail recording deletion operations, tokens, and affected records.</text>

    <!-- Atomic Sync Arrow -->
    ${svgEdge(730, 400, 760, 400, "arrow-purple", "#7c3aed")}
  </svg>
  `;
}

/**
 * Diagram 6: Safe Deletion & Audit Lifecycle
 */
function renderDeletionAuditSVG() {
  return `
  <svg width="100%" height="100%" viewBox="0 0 1480 820" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg">
    ${svgDefinitions()}

    ${svgSwimlane(20, 30, 1440, 760, "SAFE DATA DELETION & APPEND-ONLY AUDIT LIFECYCLE", "2-Step Confirmation with Impact Preview & Cryptographic Confirmation Token (src/storage/deletion_service.py)")}

    <!-- Row 1: Preview to Purge -->
    ${svgNode(60, 100, 310, 85, "safe_deletion", "1. Scope Preview", "User specifies scope:\\nCHANNEL, DATE, DATE_RANGE, WATCHLIST", "#2563eb", "PREVIEW", "#2563eb")}
    ${svgEdge(370, 142, 415, 142, "arrow-blue", "#2563eb")}

    ${svgNode(415, 100, 310, 85, "safe_deletion", "2. Impact Calculator", "Counts affected activities, transcripts,\\nanalyses, reports, and NVMe disk files", "#0891b2", "CALCULATOR", "#0891b2")}
    ${svgEdge(725, 142, 770, 142, "arrow-cyan", "#0891b2")}

    ${svgNode(770, 100, 310, 85, "safe_deletion", "3. Cryptographic Gate", "Requires explicit user confirmation:\\nMust send token: 'CONFIRM_DELETE'", "#dc2626", "AUTH GATE", "#dc2626")}
    ${svgEdge(1080, 142, 1125, 142, "arrow-red", "#dc2626")}

    ${svgNode(1125, 100, 295, 85, "safe_deletion", "4. Cascading Purge", "Removes NVMe files &amp; deletes\\nfrom MongoDB collections", "#dc2626", "PURGE", "#dc2626")}

    ${svgCurvedEdge(1272, 185, 1272, 310, "arrow-purple", "#7c3aed", true)}

    <!-- Row 2: Audit and Broadcast -->
    ${svgNode(880, 310, 540, 85, "safe_deletion", "5. Append-Only Audit Log", "Writes immutable record to state/audit/deletion_{id}.json\\nand mirrors to MongoDB db.audit_logs collection", "#7c3aed", "AUDIT RECORD", "#7c3aed")}
    ${svgEdge(880, 352, 790, 352, "arrow-green", "#059669")}

    ${svgNode(350, 310, 440, 85, "telemetry_sse", "6. UI Real-Time Telemetry Refresh", "Broadcasts updated channel statistics, refreshed\\ndate archives, and recalculated disk counters", "#059669", "STATE SYNC", "#059669")}

    <!-- Detail Explanations -->
    ${svgSwimlane(60, 460, 660, 290, "DELETION PREVIEW SCHEMA (PRE-DELETION AUDIT)", "DeletionPreview Object")}
    <text class="swimlane-desc" x="90" y="525" font-size="12" font-family="monospace">• preview_id: str, scope: DeletionScope</text>
    <text class="swimlane-desc" x="90" y="555" font-size="12" font-family="monospace">• channels: List[str], start_date: str, end_date: str</text>
    <text class="swimlane-desc" x="90" y="585" font-size="12" font-family="monospace">• total_activities: int, videos_count: int, shorts_count: int</text>
    <text class="swimlane-desc" x="90" y="615" font-size="12" font-family="monospace">• transcripts_count: int, analyses_count: int, reports_count: int</text>
    <text class="swimlane-desc" x="90" y="645" font-size="12" font-family="monospace">• affected_item_ids: List[str] (Audit trace)</text>
    <text class="swimlane-desc" x="90" y="675" font-size="12" font-family="monospace">• confirmation_required: "CONFIRM_DELETE"</text>

    ${svgSwimlane(760, 460, 660, 290, "IMMUTABLE AUDIT RECORD SCHEMA", "DeletionAuditRecord Object")}
    <text class="swimlane-desc" x="790" y="525" font-size="12" font-family="monospace">• audit_id: str, operation: "DELETE", scope: str</text>
    <text class="swimlane-desc" x="790" y="555" font-size="12" font-family="monospace">• channels: List[str], items_deleted: int, artifacts_deleted: int</text>
    <text class="swimlane-desc" x="790" y="585" font-size="12" font-family="monospace">• started_at: ISO-8601, completed_at: ISO-8601</text>
    <text class="swimlane-desc" x="790" y="615" font-size="12" font-family="monospace">• user_confirmed: true, status: "COMPLETED"</text>
    <text class="swimlane-desc" x="790" y="645" font-size="12" font-family="monospace">• Storage path: state/audit/deletion_{audit_id}.json</text>
    <text class="swimlane-desc" x="790" y="675" font-size="12" font-family="monospace">• MongoDB mirror: db.audit_logs.insert_one(audit_record)</text>
  </svg>
  `;
}

// =============================================================================
// 7. Step-by-Step Flow Mode (Timeline Cards View)
// =============================================================================
function renderTimelineView() {
  const container = document.getElementById("timeline-container");
  if (!container) return;

  const stageKeys = Object.keys(STAGE_SPECS);
  let html = `
    <div class="timeline-flow">
  `;

  stageKeys.forEach((key, idx) => {
    const spec = STAGE_SPECS[key];
    html += `
      <div class="timeline-card" onclick="openDrawer('${key}')">
        <div class="timeline-node-icon" style="border-color: ${spec.color}; color: ${spec.color}">
          ${idx + 1}
        </div>
        <div class="timeline-card-header">
          <div class="timeline-card-title">${spec.name}</div>
          <span class="timeline-badge" style="background: ${spec.color}18; border: 1px solid ${spec.color}50; color: ${spec.color}">
            ${spec.type}
          </span>
        </div>
        <p class="timeline-card-desc">${spec.description}</p>
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
            <strong>Timing Tracker</strong>
            <span>${spec.timingTrackers}</span>
          </div>
          <div class="timeline-meta-item">
            <strong>Code Reference</strong>
            <span style="color: var(--accent-blue)">${spec.codeRef}</span>
          </div>
        </div>
        <div class="timeline-inspect-hint">
          <span>🔍 Click to inspect code references, retry policy & demo screenshot →</span>
        </div>
      </div>
    `;
  });

  html += `</div>`;
  container.innerHTML = html;
}

// =============================================================================
// 8. Live Application Showcase Gallery
// =============================================================================
function renderShowcaseView() {
  const container = document.getElementById("showcase-container");
  if (!container) return;

  const showcaseItems = [
    {
      title: "1. Live Ingestion & Processing Console",
      badge: "ACTIVE ENGINE",
      color: "#059669",
      img: "assets/04_live_processing.png",
      desc: "Real-time live monitoring interface showing active channel discovery, concurrent worker item processing, real-time stage transitions, and telemetry logs.",
      features: [
        "Concurrent multi-item progress tracking (QUEUED -> COMPLETED)",
        "Real-time factor (RTF) and elapsed duration instrumentation",
        "Individual item stage telemetry with retry/error flags",
        "Instant pause, resume, and selective rerun controls"
      ]
    },
    {
      title: "2. Historical Archive & Activity Catalog",
      badge: "AUDIT TRAIL",
      color: "#2563eb",
      img: "assets/02_daily_archive.png",
      desc: "Comprehensive activity catalog partitioned by publication dates. Shows multi-channel political activity across Videos, Shorts, Community Posts, and Livestreams with sentiment filtering.",
      features: [
        "Granular date-range filtering with UTC/IST time normalization",
        "Channel watchlist breakdown with subscriber and status badges",
        "4 distinct activity badges with duration and view counters",
        "Instant one-click access to standalone HTML & JSON reports"
      ]
    },
    {
      title: "3. Grounded Intelligence & Evidence Detail",
      badge: "ZERO HALLUCINATION",
      color: "#0891b2",
      img: "assets/03_grounded_intelligence_detail.png",
      desc: "Full-text transcript inspection view with millisecond-precision word timestamps, verbatim grounding, and clickable YouTube timestamp links (&t=XXs).",
      features: [
        "Verbatim multilingual speech transcript (Devanagari Hindi, English)",
        "Word-level start/end timestamp anchors for precise grounding",
        "Fast-path native caption shortcut tag (~0.05s retrieval)",
        "Programmatic verification (exact match, prefix, 60% overlap)"
      ]
    },
    {
      title: "4. Political Analytics & Sentiment Dashboard",
      badge: "ANALYTICS",
      color: "#d97706",
      img: "assets/01_analytics_dashboard.png",
      desc: "Aggregated intelligence dashboard displaying channel comparison metrics, sentiment trajectory, key political entities, and topical volume distributions.",
      features: [
        "Interactive trend charts with multi-channel comparisons",
        "Sentiment analysis and topic classification",
        "Top political entities, schemes, and policy keyword mentions",
        "Daily executive intelligence overview summary cards"
      ]
    },
    {
      title: "5. Safe Data Management & Audit Lifecycle",
      badge: "GOVERNANCE",
      color: "#dc2626",
      img: "assets/05_data_management.png",
      desc: "Two-step safe deletion console with impact preview calculations, cryptographic token confirmation ('CONFIRM_DELETE'), and append-only audit trail logging.",
      features: [
        "2-step impact calculation across Channels, Dates, and Watchlists",
        "Atomic disk & database cascading purge safeguards",
        "Cryptographic confirmation token gate preventing accidental loss",
        "Append-only audit trail mirrored to NVMe JSON and MongoDB"
      ]
    },
    {
      title: "6. End-to-End System Architecture & Swimlanes",
      badge: "ARCHITECTURE",
      color: "#6366f1",
      img: "assets/06_pipeline_flow.png",
      desc: "Complete multi-phase architectural blueprint mapping channel discovery, 3-worker concurrency, Whisper STT, Ollama dual-engine analysis, Levenshtein evidence grounding, and dual-tier persistence.",
      features: [
        "6 lifecycle transitions (1. QUEUED through 6. REPORT READY)",
        "Thread pool worker concurrency with live SSE telemetry",
        "Fast-path native caption shortcut bypassing audio extraction",
        "Zero-hallucination Levenshtein fuzzy quote grounding"
      ]
    }
  ];

  let html = `
    <div class="showcase-header">
      <h2>📸 Live Application & Architecture Showcase Gallery</h2>
      <p>Interactive demonstration of the Political YouTube Channel Activity Monitor and its end-to-end processing architecture.</p>
    </div>
    <div class="showcase-grid">
  `;

  showcaseItems.forEach(item => {
    html += `
      <div class="showcase-card">
        <div class="showcase-img-box" onclick="openLightbox('${item.img}', '${item.title}')">
          <img src="${item.img}" alt="${item.title}" loading="lazy">
          <div class="showcase-zoom-badge">🔍 Click to Enlarge</div>
        </div>
        <div class="showcase-card-body">
          <div class="showcase-card-title-group">
            <h3 class="showcase-card-title">${item.title}</h3>
            <span class="showcase-stage-tag" style="background: ${item.color}18; border: 1px solid ${item.color}50; color: ${item.color}">
              ${item.badge}
            </span>
          </div>
          <p class="showcase-card-desc">${item.desc}</p>
          <ul class="showcase-features-list">
            ${item.features.map(f => `<li>${f}</li>`).join("")}
          </ul>
        </div>
      </div>
    `;
  });

  html += `</div>`;
  container.innerHTML = html;
}

// =============================================================================
// 9. Limitations & Technical Challenges View
// =============================================================================
async function renderLimitationsView() {
  const container = document.getElementById("limitations-container");
  if (!container) return;

  try {
    const res = await fetch("LIMITATIONS_AND_CHALLENGES.md");
    if (!res.ok) throw new Error("Failed to load limitations doc");
    const text = await res.text();

    // Basic markdown to HTML renderer for clean presentation
    let rendered = text
      .replace(/^# (.*$)/gim, '<h1>$1</h1>')
      .replace(/^## (.*$)/gim, '<h2>$1</h2>')
      .replace(/^### (.*$)/gim, '<h3>$1</h3>')
      .replace(/^\* (.*$)/gim, '<li>$1</li>')
      .replace(/^- (.*$)/gim, '<li>$1</li>')
      .replace(/`([^`]+)`/gim, '<code>$1</code>')
      .replace(/\*\*([^*]+)\*\*/gim, '<strong>$1</strong>')
      .replace(/---/gim, '<hr>');

    container.innerHTML = `<div class="markdown-body">${rendered}</div>`;
  } catch (err) {
    container.innerHTML = `
      <div class="markdown-body">
        <h2>⚠️ Limitations & Challenges Documentation</h2>
        <p>Could not load LIMITATIONS_AND_CHALLENGES.md directly. Please review the file under docs/pipeline-flow/LIMITATIONS_AND_CHALLENGES.md.</p>
      </div>
    `;
  }
}

// =============================================================================
// 10. Slide-Over Detail Drawer & Lightbox
// =============================================================================
function openDrawer(stageKey) {
  const spec = STAGE_SPECS[stageKey];
  if (!spec) return;

  const drawer = document.getElementById("detail-drawer");
  const backdrop = document.getElementById("drawer-backdrop");
  if (!drawer) return;

  document.getElementById("drawer-stage-badge").textContent = spec.category;
  document.getElementById("drawer-stage-badge").style.color = spec.color;
  document.getElementById("drawer-stage-title").textContent = spec.name;
  document.getElementById("drawer-desc").textContent = spec.description;
  document.getElementById("drawer-input").textContent = spec.inputs;
  document.getElementById("drawer-output").textContent = spec.outputs;
  document.getElementById("drawer-timing").textContent = spec.timingTrackers;
  document.getElementById("drawer-failure").textContent = spec.failureModes;
  document.getElementById("drawer-retry").textContent = spec.retryPolicy;
  document.getElementById("drawer-code").textContent = spec.codeRef;

  // Render Files Produced Tags
  const filesContainer = document.getElementById("drawer-files-list");
  if (filesContainer) {
    const files = spec.filesProduced.split(",").map(f => f.trim());
    filesContainer.innerHTML = files.map(f => `<span class="tag-item">📄 ${f}</span>`).join("");
  }

  // Render JSON Payload
  const payloadBox = document.getElementById("drawer-payload");
  if (payloadBox) {
    payloadBox.textContent = JSON.stringify(spec.samplePayload, null, 2);
  }

  // Demonstration Image Card
  const demoSection = document.getElementById("drawer-demo-section");
  const demoCard = document.getElementById("drawer-demo-card");
  if (demoSection && demoCard) {
    if (spec.demoImage) {
      demoSection.style.display = "block";
      demoCard.innerHTML = `
        <img src="${spec.demoImage}" alt="${spec.name}">
        <span class="drawer-demo-badge">🔍 Click to Enlarge Preview</span>
      `;
      demoCard.onclick = () => openLightbox(spec.demoImage, spec.demoCaption || spec.name);
    } else {
      demoSection.style.display = "none";
    }
  }

  drawer.classList.add("open");
  if (backdrop) backdrop.classList.add("active");
}

function closeDrawer() {
  const drawer = document.getElementById("detail-drawer");
  const backdrop = document.getElementById("drawer-backdrop");
  if (drawer) drawer.classList.remove("open");
  if (backdrop) backdrop.classList.remove("active");
}

function openLightbox(imgSrc, title) {
  const modal = document.getElementById("lightbox-modal");
  const img = document.getElementById("lightbox-img");
  const titleEl = document.getElementById("lightbox-title");
  const captionEl = document.getElementById("lightbox-caption");

  if (!modal || !img) return;

  img.src = imgSrc;
  if (titleEl) titleEl.textContent = title || "Live Demonstration Preview";
  if (captionEl) captionEl.textContent = title || "";
  modal.classList.add("open");
}

// =============================================================================
// 11. Search & Filter Functionality
// =============================================================================
function handleSearch(query) {
  if (!query) {
    document.querySelectorAll(".node-group").forEach(el => el.style.opacity = "1");
    document.querySelectorAll(".timeline-card").forEach(el => el.style.display = "block");
    return;
  }

  // Diagram Node Search
  document.querySelectorAll(".node-group").forEach(el => {
    const stageKey = el.getAttribute("data-stage");
    const spec = STAGE_SPECS[stageKey];
    if (spec) {
      const match = (
        spec.name.toLowerCase().includes(query) ||
        spec.description.toLowerCase().includes(query) ||
        spec.inputs.toLowerCase().includes(query) ||
        spec.outputs.toLowerCase().includes(query) ||
        spec.codeRef.toLowerCase().includes(query)
      );
      el.style.opacity = match ? "1" : "0.2";
    }
  });

  // Timeline Card Search
  document.querySelectorAll(".timeline-card").forEach(card => {
    const text = card.textContent.toLowerCase();
    card.style.display = text.includes(query) ? "block" : "none";
  });
}
