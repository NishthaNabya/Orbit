"""
server.py — Orbit V2 Processing Layer
FastAPI server with async queue, file locking, and background worker.
"""

import asyncio
import json
import logging
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import trafilatura
import yt_dlp
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from youtube_transcript_api import YouTubeTranscriptApi

# Load environment variables from .env file
load_dotenv()

# ──────────────────────────────────────────────
# Gemini Client (google-genai SDK)
# ──────────────────────────────────────────────
# Uses GEMINI_API_KEY from env automatically. The new SDK is client-based,
# not module-level — one client instance is shared across all calls.
gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Model selection by content type:
# - 2.5-flash for text (tweets, articles): fast, cheap, plenty for tag+summary extraction
# - 2.5-pro for video: the reasoning headroom is worth the cost when analyzing 10+ min of footage
TEXT_MODEL = "gemini-2.5-flash"
VIDEO_MODEL = "gemini-2.5-pro"

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

VAULT_DIR = Path(__file__).parent / "vault"
BOOKMARKS_FILE = VAULT_DIR / "bookmarks.md"
TRANSCRIPTS_FILE = VAULT_DIR / "transcripts.md"
INGEST_LOG_DB = VAULT_DIR / "ingest.log"

MAX_WORKER_CONCURRENCY = 3
RETRY_DELAY_SECONDS = 5

VAULT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("orbit")

# ──────────────────────────────────────────────
# Pydantic Models
# ──────────────────────────────────────────────


class IngestPayload(BaseModel):
    type: str = Field(..., description="tweet, thread, youtube, article, podcast")
    source_url: str = Field(..., description="Canonical URL of the source")
    source_platform: str = Field(..., description="x, youtube, medium, substack, other")
    author: str = Field(default="", description="Display name")
    author_handle: str = Field(default="", description="Normalized handle")
    title: str = Field(default="", description="Headline or first line")
    captured_at: str = Field(..., description="ISO 8601 timestamp of capture")
    published_at: Optional[str] = Field(default=None, description="Original publish date")
    content: str = Field(default="", description="Raw text content")
    selection: Optional[str] = Field(default=None, description="User-selected text if any")


class EnrichmentResult(BaseModel):
    """
    Schema enforced on Gemini's response. Using response_schema with this Pydantic
    model means Gemini returns guaranteed-valid JSON matching this shape — no more
    regex-fishing for {...} in the response text.
    """
    tags: list[str] = Field(default_factory=list, description="3-7 lowercase topical tags")
    summary: str = Field(default="", description="One-sentence summary of the core idea")
    key_insights: list[str] = Field(default_factory=list, description="2-5 standalone insights")


# ──────────────────────────────────────────────
# Async resources (created during lifespan startup)
# ──────────────────────────────────────────────

# Async queue for non-blocking ingestion
ingest_queue: asyncio.Queue = asyncio.Queue()

# File locks for safe concurrent appends
file_lock = asyncio.Lock()
transcript_lock = asyncio.Lock()

# LLM concurrency semaphore — caps simultaneous Gemini calls
llm_semaphore = asyncio.Semaphore(MAX_WORKER_CONCURRENCY)

# ──────────────────────────────────────────────
# Ingest Log (SQLite — metadata only)
# ──────────────────────────────────────────────


def init_ingest_log():
    """Initialize SQLite metadata log for tracking ingest status and retries."""
    conn = sqlite3.connect(str(INGEST_LOG_DB))
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_log (
            id TEXT PRIMARY KEY,
            source_url TEXT NOT NULL,
            type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            retries INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def log_ingest_entry(entry_id: str, source_url: str, entry_type: str, status: str, retries: int = 0):
    """Insert or update an ingest log entry."""
    conn = sqlite3.connect(str(INGEST_LOG_DB))
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute(
        """
        INSERT INTO ingest_log (id, source_url, type, status, retries, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            retries = excluded.retries,
            updated_at = excluded.updated_at
        """,
        (entry_id, source_url, entry_type, status, retries, now, now),
    )
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────
# Markdown Template Engine
# ──────────────────────────────────────────────


def format_bookmark_entry(
    payload: IngestPayload,
    entry_id: str,
    tags: list = None,
    summary: str = None,
    key_insights: list = None,
    status: str = "ingested",
    content_header: str = "Content",
) -> str:
    """Render a bookmark entry with YAML frontmatter and Markdown body."""
    tags_json = json.dumps(tags or [])
    insights_json = "\n".join(f"  - {insight}" for insight in (key_insights or []))
    if not insights_json:
        insights_json = "  []"

    frontmatter = f"""---
id: "{entry_id}"
type: "{payload.type}"
source_url: "{payload.source_url}"
source_platform: "{payload.source_platform}"
author: "{_escape_yaml(payload.author)}"
author_handle: "{_escape_yaml(payload.author_handle)}"
title: "{_escape_yaml(payload.title)}"
captured_at: "{payload.captured_at}"
published_at: {payload.published_at if payload.published_at else "null"}
tags: {tags_json}
summary: {_format_yaml_string(summary)}
key_insights:
{insights_json}
status: "{status}"
---

## {content_header}

{payload.content}

## Context

- **Original URL:** {payload.source_url}
- **Captured:** {payload.captured_at}
- **Platform:** {payload.source_platform}

---

"""
    return frontmatter


def _escape_yaml(value: str) -> str:
    """Escape double quotes for safe YAML embedding."""
    return value.replace('"', '\\"').replace("\n", " ") if value else ""


def _format_yaml_string(value: Optional[str]) -> str:
    """Format a string value for YAML — quote if present, null if not."""
    if value:
        return f'"{_escape_yaml(value)}"'
    return "null"


# ──────────────────────────────────────────────
# Article Content Extraction
# ──────────────────────────────────────────────


async def extract_article_content(url: str) -> Optional[str]:
    """
    Fetch a URL and extract clean article text using trafilatura.

    Runs in a thread pool to avoid blocking the async event loop,
    since trafilatura is a synchronous library.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _trafilatura_extract, url)


def _trafilatura_extract(url: str) -> Optional[str]:
    """Synchronous wrapper for trafilatura extraction."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded is None:
            logger.warning(f"[Extractor] Failed to fetch URL: {url}")
            return None

        extracted = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=True,
            output_format="txt",
        )

        if extracted and len(extracted.strip()) > 50:
            logger.info(f"[Extractor] Extracted {len(extracted)} chars from {url}")
            return extracted.strip()
        else:
            logger.warning(f"[Extractor] Extraction returned empty/short text for {url}")
            return None
    except Exception as e:
        logger.error(f"[Extractor] Error extracting {url}: {e}")
        return None


# ──────────────────────────────────────────────
# YouTube Processing Pipeline
# ──────────────────────────────────────────────

VIDEOS_DIR = VAULT_DIR / "videos"
VIDEOS_DIR.mkdir(exist_ok=True)


def extract_video_id(url: str) -> Optional[str]:
    """Extract the YouTube video ID from various URL formats."""
    import re

    patterns = [
        r"(?:v=|/v/|youtu\.be/|/embed/|/shorts/)([a-zA-Z0-9_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


async def extract_youtube_metadata(url: str) -> dict:
    """Use yt-dlp to extract real metadata from a YouTube video."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _yt_dlp_metadata, url)


def _yt_dlp_metadata(url: str) -> dict:
    """Synchronous wrapper for yt-dlp metadata extraction."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "skip_download": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return {
                "title": info.get("title", ""),
                "channel": info.get("channel", info.get("uploader", "")),
                "upload_date": info.get("upload_date", ""),
                "duration": info.get("duration", 0),
                "description": info.get("description", ""),
                "view_count": info.get("view_count", 0),
            }
    except Exception as e:
        logger.error(f"[YouTube] yt-dlp metadata extraction failed for {url}: {e}")
        return {}


async def get_youtube_transcript(video_id: str) -> Optional[str]:
    """Fetch the full transcript for a YouTube video."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_transcript, video_id)


def _fetch_transcript(video_id: str) -> Optional[str]:
    """Synchronous wrapper for transcript fetching."""
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Prefer manually created transcripts, fall back to auto-generated
        try:
            transcript = transcript_list.find_manually_created_transcript()
        except Exception:
            try:
                transcript = transcript_list.find_generated_transcript()
            except Exception:
                # Last resort: fetch the first available transcript in any language
                transcript = transcript_list.find_transcript(
                    transcript_list._transcripts[0][0]
                )

        segments = transcript.fetch()
        full_text = "\n".join(entry["text"] for entry in segments)

        if full_text.strip():
            logger.info(f"[YouTube] Transcript fetched for {video_id} ({len(full_text)} chars)")
            return full_text
        return None
    except Exception as e:
        logger.warning(f"[YouTube] No transcript available for {video_id}: {e}")
        return None


async def download_video_locally(url: str, video_id: str) -> Optional[str]:
    """Download the best quality MP4 of a YouTube video to vault/videos/."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _yt_dlp_download, url, video_id)


def _yt_dlp_download(url: str, video_id: str) -> Optional[str]:
    """Synchronous wrapper for yt-dlp video download."""
    output_template = str(VIDEOS_DIR / "%(id)s_%(title).100s.%(ext)s")

    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
        "noplaylist": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            if filepath:
                logger.info(f"[YouTube] Video saved to {filepath}")
                return filepath
            return None
    except Exception as e:
        logger.error(f"[YouTube] Video download failed for {video_id}: {e}")
        return None


# ──────────────────────────────────────────────
# LLM Enrichment (google-genai SDK)
# ──────────────────────────────────────────────

ENRICHMENT_PROMPT = """You are a research librarian organizing someone's personal knowledge graph. Analyze this content and extract:

- 3-7 lowercase topical tags (single words or hyphenated phrases, no spaces)
- A one-sentence summary capturing the core idea in the author's voice
- 2-5 standalone insights — each should be readable on its own, without needing the original

Be concrete. Avoid filler like "discusses" or "talks about". If the content is thin, return fewer tags and insights rather than padding."""


async def enrich_with_llm(payload: IngestPayload) -> dict:
    """
    Enrich content with Gemini, returning structured tags/summary/insights.

    For YouTube videos with a local MP4 available, uploads the file and uses
    the multimodal pro model. For everything else, uses text-only flash.

    Uses response_schema to guarantee valid JSON output — no regex parsing,
    no markdown-fenced JSON to strip, no fallback-on-malformed-output paths.
    """
    async with llm_semaphore:
        try:
            is_youtube = (
                payload.source_platform == "youtube"
                or "youtube.com" in payload.source_url
                or "youtu.be" in payload.source_url
            )

            generation_config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=EnrichmentResult,
                system_instruction=ENRICHMENT_PROMPT,
            )

            # ── Video path: upload MP4, wait for processing, then analyze ──
            if is_youtube:
                video_id = extract_video_id(payload.source_url)
                if video_id:
                    video_files = list(VIDEOS_DIR.glob(f"{video_id}_*.mp4"))
                    if video_files:
                        video_path = video_files[0]
                        logger.info(f"[Gemini] Uploading local video: {video_path.name}")

                        # Upload via async client. The new SDK uses client.aio.files.upload
                        uploaded = await gemini_client.aio.files.upload(
                            file=str(video_path)
                        )
                        logger.info(f"[Gemini] Uploaded: {uploaded.name}")

                        # Poll until ACTIVE — Gemini processes video before it can be referenced
                        while uploaded.state.name == "PROCESSING":
                            logger.info("[Gemini] Waiting for video processing…")
                            await asyncio.sleep(2)
                            uploaded = await gemini_client.aio.files.get(name=uploaded.name)

                        if uploaded.state.name == "ACTIVE":
                            response = await gemini_client.aio.models.generate_content(
                                model=VIDEO_MODEL,
                                contents=[
                                    "Analyze this video for my knowledge graph.",
                                    uploaded,
                                ],
                                config=generation_config,
                            )

                            # Cleanup: delete uploaded file from Gemini's temp storage
                            try:
                                await gemini_client.aio.files.delete(name=uploaded.name)
                            except Exception as cleanup_err:
                                logger.warning(f"[Gemini] File cleanup failed: {cleanup_err}")

                            return _parse_structured_response(response)
                        else:
                            logger.error(f"[Gemini] Video processing failed: {uploaded.state.name}")
                    else:
                        logger.warning(f"[Gemini] No local video found for {video_id}, falling back to text")
                else:
                    logger.warning(f"[Gemini] Could not extract video ID from {payload.source_url}")

            # ── Text path: tweet, article, or YouTube fallback to transcript text ──
            content_for_analysis = payload.content or payload.title or payload.source_url
            if not content_for_analysis.strip():
                logger.warning("[Gemini] No content to analyze, returning empty enrichment")
                return _empty_enrichment()

            response = await gemini_client.aio.models.generate_content(
                model=TEXT_MODEL,
                contents=content_for_analysis,
                config=generation_config,
            )
            return _parse_structured_response(response)

        except Exception as e:
            logger.error(f"[Gemini] Enrichment failed: {e}")
            return _empty_enrichment()


def _parse_structured_response(response) -> dict:
    """
    Parse a Gemini response that was constrained by response_schema.

    With response_schema set, response.parsed is the typed Pydantic instance.
    response.text is the raw JSON. We try parsed first, fall back to text.
    """
    try:
        # The SDK populates .parsed when response_schema is set
        if hasattr(response, "parsed") and response.parsed is not None:
            result: EnrichmentResult = response.parsed
            logger.info(f"[Gemini] Enriched: {len(result.tags)} tags, {len(result.key_insights)} insights")
            return {
                "tags": result.tags,
                "summary": result.summary or None,
                "key_insights": result.key_insights,
            }

        # Fallback: parse the text directly
        data = json.loads(response.text)
        return {
            "tags": data.get("tags", []),
            "summary": data.get("summary") or None,
            "key_insights": data.get("key_insights", []),
        }
    except (json.JSONDecodeError, AttributeError) as e:
        logger.error(f"[Gemini] Failed to parse structured response: {e}")
        return _empty_enrichment()


def _empty_enrichment() -> dict:
    return {"tags": [], "summary": None, "key_insights": []}


# ──────────────────────────────────────────────
# Background Worker
# ──────────────────────────────────────────────


async def background_worker():
    """
    Persistent background coroutine that processes the ingest queue.

    For each payload:
    1. Pop from queue
    2. Generate UUID and log as 'processing'
    3. Type-specific extraction (YouTube transcript/metadata/video, article fetch)
    4. Gemini enrichment
    5. Append to vault file (bookmarks.md or transcripts.md) under file lock
    6. Log final status to SQLite
    """
    logger.info("[Worker] Background worker started.")

    while True:
        try:
            payload: IngestPayload = await ingest_queue.get()
            entry_id = str(uuid.uuid4())

            # Compute once per iteration — used in extraction, enrichment, and routing
            is_youtube = (
                payload.source_platform == "youtube"
                or "youtube.com" in payload.source_url
                or "youtu.be" in payload.source_url
            )

            logger.info(f"[Worker] Processing: {payload.source_url} (id={entry_id})")
            log_ingest_entry(entry_id, payload.source_url, payload.type, "processing")

            try:
                # ── YouTube: transcript, metadata, video download ──
                if is_youtube:
                    video_id = extract_video_id(payload.source_url)
                    if video_id:
                        logger.info(f"[Worker] YouTube video detected: {video_id}")

                        if not payload.content.strip():
                            transcript = await get_youtube_transcript(video_id)
                            if transcript:
                                payload.content = transcript
                                logger.info(f"[Worker] Transcript fetched: {len(transcript)} chars")
                            else:
                                payload.content = (
                                    f"[No transcript available for this video]\n\nURL: {payload.source_url}"
                                )
                                logger.warning(f"[Worker] No transcript for {video_id}")

                        metadata = await extract_youtube_metadata(payload.source_url)
                        if metadata:
                            if metadata.get("title") and not payload.title:
                                payload.title = metadata["title"]
                            if metadata.get("channel") and not payload.author:
                                payload.author = metadata["channel"]
                                payload.author_handle = metadata["channel"].lower().replace(" ", "_")
                            if metadata.get("upload_date"):
                                payload.published_at = metadata["upload_date"]
                            logger.info(f"[Worker] YouTube metadata enriched: {payload.title} by {payload.author}")

                        # Fire-and-forget: video download runs in parallel with enrichment.
                        # The bookmark gets saved either way.
                        asyncio.create_task(download_video_locally(payload.source_url, video_id))

                # ── Article: full-text extraction via trafilatura ──
                elif payload.type == "article" and not payload.content.strip():
                    logger.info(f"[Worker] Article has no content. Extracting from {payload.source_url}")
                    extracted = await extract_article_content(payload.source_url)

                    if extracted:
                        payload.content = extracted
                        logger.info(f"[Worker] Article extraction succeeded: {len(extracted)} chars")
                    elif payload.selection:
                        payload.content = f"[User selection]\n\n{payload.selection}"
                        logger.info(f"[Worker] Falling back to user selection: {len(payload.selection)} chars")
                    else:
                        payload.content = (
                            f"[Failed to extract content from {payload.source_url}]\n\n"
                            "The page could not be parsed. The URL has been saved for reference."
                        )
                        logger.warning(f"[Worker] Article extraction failed: {payload.source_url}")

                # ── Enrichment ──
                enrichment = await enrich_with_llm(payload)

                # ── Route to correct vault file ──
                if is_youtube:
                    content_header = (
                        "Transcript"
                        if payload.content and "[No transcript" not in payload.content
                        else "Content"
                    )
                    entry = format_bookmark_entry(
                        payload=payload,
                        entry_id=entry_id,
                        tags=enrichment.get("tags", []),
                        summary=enrichment.get("summary"),
                        key_insights=enrichment.get("key_insights", []),
                        status="enriched",
                        content_header=content_header,
                    )
                    async with transcript_lock:
                        async with aiofiles.open(str(TRANSCRIPTS_FILE), mode="a", encoding="utf-8") as f:
                            await f.write(entry)
                    logger.info(f"[Worker] Enriched and saved to transcripts.md: {payload.title}")
                else:
                    entry = format_bookmark_entry(
                        payload=payload,
                        entry_id=entry_id,
                        tags=enrichment.get("tags", []),
                        summary=enrichment.get("summary"),
                        key_insights=enrichment.get("key_insights", []),
                        status="enriched",
                    )
                    async with file_lock:
                        async with aiofiles.open(str(BOOKMARKS_FILE), mode="a", encoding="utf-8") as f:
                            await f.write(entry)
                    logger.info(f"[Worker] Enriched and saved to bookmarks.md: {payload.title}")

                log_ingest_entry(entry_id, payload.source_url, payload.type, "enriched")

            except Exception as e:
                logger.error(f"[Worker] Processing failed for {payload.source_url}: {e}")

                # On failure, still save a stub entry so the URL isn't lost
                entry = format_bookmark_entry(
                    payload=payload,
                    entry_id=entry_id,
                    status="failed",
                )

                if is_youtube:
                    async with transcript_lock:
                        async with aiofiles.open(str(TRANSCRIPTS_FILE), mode="a", encoding="utf-8") as f:
                            await f.write(entry)
                else:
                    async with file_lock:
                        async with aiofiles.open(str(BOOKMARKS_FILE), mode="a", encoding="utf-8") as f:
                            await f.write(entry)

                log_ingest_entry(entry_id, payload.source_url, payload.type, "failed")

            finally:
                ingest_queue.task_done()

        except Exception as e:
            logger.error(f"[Worker] Unexpected error in worker loop: {e}")
            await asyncio.sleep(1)


# ──────────────────────────────────────────────
# FastAPI App + Lifespan
# ──────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Replaces the deprecated @app.on_event("startup") pattern.
    Initializes SQLite log and spawns the background worker on startup.
    The worker task runs until shutdown, when it's cancelled cleanly.
    """
    init_ingest_log()
    worker_task = asyncio.create_task(background_worker())
    logger.info("[Server] Orbit V2 ingestion server started.")

    yield

    # Shutdown: cancel the worker, wait for in-flight items
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    logger.info("[Server] Orbit V2 ingestion server stopped.")


app = FastAPI(title="Orbit Ingestion Server", version="2.0.0", lifespan=lifespan)


# ──────────────────────────────────────────────
# API Endpoints
# ──────────────────────────────────────────────


@app.post("/ingest", status_code=202)
async def ingest(payload: IngestPayload):
    """
    Accept a normalized bookmark payload and queue it for async processing.

    Returns 202 Accepted immediately — the user never waits for LLM enrichment
    or video download. Processing happens in the background worker.
    """
    await ingest_queue.put(payload)
    logger.info(f"[API] Queued: {payload.source_url} (queue size: {ingest_queue.qsize()})")
    return {"status": "accepted", "queue_size": ingest_queue.qsize()}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "queue_size": ingest_queue.qsize(),
        "bookmarks_file": str(BOOKMARKS_FILE),
        "bookmarks_exists": BOOKMARKS_FILE.exists(),
        "transcripts_file": str(TRANSCRIPTS_FILE),
        "transcripts_exists": TRANSCRIPTS_FILE.exists(),
    }


@app.get("/status/{entry_id}")
async def get_status(entry_id: str):
    """Look up the processing status of a specific bookmark by UUID."""
    conn = sqlite3.connect(str(INGEST_LOG_DB))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, source_url, type, status, retries, created_at, updated_at FROM ingest_log WHERE id = ?",
        (entry_id,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Entry not found")

    return {
        "id": row[0],
        "source_url": row[1],
        "type": row[2],
        "status": row[3],
        "retries": row[4],
        "created_at": row[5],
        "updated_at": row[6],
    }