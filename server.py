"""
server.py — Orbit V2 Processing Layer
FastAPI server with async queue, file locking, and background worker.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import google.generativeai as genai
import trafilatura
import yt_dlp
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Load environment variables from .env file
load_dotenv()

# Configure Gemini API
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────

VAULT_DIR = Path(__file__).parent / "vault"
BOOKMARKS_FILE = VAULT_DIR / "bookmarks.md"
TRANSCRIPTS_FILE = VAULT_DIR / "transcripts.md"
INGEST_LOG_DB = VAULT_DIR / "ingest.log"
TRANSCRIPTS_FILE = VAULT_DIR / "transcripts.md"
transcript_lock = asyncio.Lock()  
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


# ──────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────

app = FastAPI(title="Orbit Ingestion Server", version="2.0.0")

# Async queue for non-blocking ingestion
ingest_queue: asyncio.Queue = asyncio.Queue()

# File lock for safe concurrent appends to bookmarks.md
file_lock = asyncio.Lock()

# File lock for safe concurrent appends to transcripts.md
transcript_lock = asyncio.Lock()

# LLM concurrency semaphore
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


def format_bookmark_entry(payload: IngestPayload, entry_id: str, tags: list = None, summary: str = None, key_insights: list = None, status: str = "ingested", content_header: str = "Content") -> str:
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

    Returns the cleaned text, or None if extraction fails.
    """
    loop = asyncio.get_event_loop()
    content = await loop.run_in_executor(
        None, _trafilatura_extract, url
    )
    return content


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
    """
    Extract the YouTube video ID from various URL formats.

    Handles:
    - https://www.youtube.com/watch?v=VIDEO_ID
    - https://youtu.be/VIDEO_ID
    - https://www.youtube.com/embed/VIDEO_ID
    - https://www.youtube.com/shorts/VIDEO_ID
    """
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
    """
    Use yt-dlp to extract real metadata from a YouTube video:
    title, channel name, upload date, duration, description.

    Runs in a thread pool since yt-dlp is synchronous.
    """
    loop = asyncio.get_event_loop()
    metadata = await loop.run_in_executor(
        None, _yt_dlp_metadata, url
    )
    return metadata


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
    """
    Fetch the full transcript for a YouTube video using youtube-transcript-api.

    Runs in a thread pool since the library is synchronous.
    Returns the transcript as a single text block, or None if unavailable.
    """
    loop = asyncio.get_event_loop()
    transcript = await loop.run_in_executor(
        None, _fetch_transcript, video_id
    )
    return transcript


def _fetch_transcript(video_id: str) -> Optional[str]:
    """Synchronous wrapper for transcript fetching."""
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Prefer manually created transcripts, fall back to auto-generated
        try:
            transcript = transcript_list.find_manually_created_transcript()
        except:
            try:
                transcript = transcript_list.find_generated_transcript()
            except:
                # Last resort: fetch the first available transcript in any language
                transcript = transcript_list.find_transcript(transcript_list._transcripts[0][0])

        # Fetch and join all segments
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
    """
    Download the best quality MP4 of a YouTube video to vault/videos/.

    Runs in a thread pool since yt-dlp is synchronous.
    Returns the path to the downloaded file, or None on failure.

    This is a fire-and-forget background task — it does not block
    the ingest pipeline. The bookmark is saved to the vault regardless
    of whether the video download succeeds.
    """
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _yt_dlp_download, url, video_id
    )
    return result


def _yt_dlp_download(url: str, video_id: str) -> Optional[str]:
    """Synchronous wrapper for yt-dlp video download."""
    output_template = str(VIDEOS_DIR / f"%(id)s_%(title).100s.%(ext)s")

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
            # yt-dlp may produce .mkv if merge fails, handle gracefully
            if filepath:
                logger.info(f"[YouTube] Video saved to {filepath}")
                return filepath
            return None
    except Exception as e:
        logger.error(f"[YouTube] Video download failed for {video_id}: {e}")
        return None


# ──────────────────────────────────────────────
# LLM Enrichment (Stub — plug in your provider)
# ──────────────────────────────────────────────


async def enrich_with_llm(payload: IngestPayload) -> dict:
    """
    Use Google Gemini to analyze content and extract structured insights.
    
    For YouTube videos: Upload local MP4 file and analyze with Gemini vision
    For text content (tweets/articles): Send content as text prompt
    
    Returns dict with tags, summary, key_insights, and optionally transcript for videos.
    """
    async with llm_semaphore:
        try:
            # Determine if this is YouTube content
            is_youtube = (
                payload.source_platform == "youtube"
                or "youtube.com" in payload.source_url
                or "youtu.be" in payload.source_url
            )
            
            # Prepare the prompt for Gemini
            prompt = """You are a Research Librarian. Analyze this content and return a JSON object with: 
            {tags: [], summary: '', key_insights: []}. 
            If this is a video, also include a full 'transcript' string in the JSON."""
            
            if is_youtube:
                # For YouTube: Try to find and upload local video file
                video_id = extract_video_id(payload.source_url)
                if video_id:
                    # Look for existing video file in vault/videos/
                    video_files = list(VIDEOS_DIR.glob(f"{video_id}_*.mp4"))
                    if video_files:
                        video_path = video_files[0]  # Take the first match
                        logger.info(f"[Gemini] Found local video: {video_path.name}")
                        
                        # Upload the video file to Gemini
                        video_file = genai.upload_file(path=str(video_path))
                        logger.info(f"[Gemini] Uploaded video file: {video_file.uri}")
                        
                        # Wait for the video to be processed
                        while video_file.state.name == "PROCESSING":
                            logger.info("[Gemini] Waiting for video to be processed...")
                            await asyncio.sleep(2)
                            video_file = genai.get_file(video_file.name)
                        
                        if video_file.state.name == "ACTIVE":
                            # Analyze the video with Gemini
                            model = genai.GenerativeModel('gemini-1.5-pro-latest')
                            response = await model.generate_content_async([prompt, video_file])
                            
                            # Clean up: delete the uploaded file
                            genai.delete_file(video_file.name)
                            logger.info(f"[Gemini] Deleted uploaded file: {video_file.name}")
                            
                            # Parse and return the response
                            return _parse_gemini_response(response.text)
                        else:
                            logger.error(f"[Gemini] Video processing failed: {video_file.state.name}")
                    else:
                        logger.warning(f"[Gemini] No local video found for {video_id}")
                else:
                    logger.warning(f"[Gemini] Could not extract video ID from {payload.source_url}")
            
            # For text content (or if YouTube video processing failed): Use text prompt
            logger.info("[Gemini] Using text-only analysis")
            model = genai.GenerativeModel('gemini-1.5-pro-latest')
            response = await model.generate_content_async([prompt, payload.content])
            return _parse_gemini_response(response.text)
            
        except Exception as e:
            logger.error(f"[Gemini] Error during analysis: {e}")
            # Fallback to stub response on error
            return {
                "tags": [],
                "summary": None,
                "key_insights": [],
            }


def _parse_gemini_response(response_text: str) -> dict:
    """Parse Gemini's JSON response into a dict."""
    import re
    
    try:
        # Try to extract JSON from the response
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            result = json.loads(json_str)
            
            # Ensure required fields exist
            result.setdefault("tags", [])
            result.setdefault("summary", None)
            result.setdefault("key_insights", [])
            
            logger.info(f"[Gemini] Successfully parsed response: {result}")
            return result
        else:
            logger.warning("[Gemini] No JSON found in response")
            return {
                "tags": [],
                "summary": None,
                "key_insights": [],
            }
    except json.JSONDecodeError as e:
        logger.error(f"[Gemini] Failed to parse JSON: {e}")
        return {
            "tags": [],
            "summary": None,
            "key_insights": [],
        }


# ──────────────────────────────────────────────
# File Writer
# ──────────────────────────────────────────────


async def append_to_vault(entry: str):
    """Append a formatted bookmark entry to vault/bookmarks.md with an async lock."""
    async with file_lock:
        async with aiofiles.open(str(BOOKMARKS_FILE), mode="a", encoding="utf-8") as f:
            await f.write(entry)
        logger.info(f"[Vault] Appended entry to {BOOKMARKS_FILE}")


# ──────────────────────────────────────────────
# Background Worker
# ──────────────────────────────────────────────


async def background_worker():
    """
    Persistent background coroutine that processes the ingest queue.

    For each payload:
    1. Pop from queue
    2. Generate UUID
    3. Call LLM for enrichment
    4. Format as Markdown with frontmatter
    5. Append to vault/bookmarks.md (with file lock)
    6. Log status to SQLite
    """
    logger.info("[Worker] Background worker started.")

    while True:
        try:
            payload: IngestPayload = await ingest_queue.get()
            entry_id = str(uuid.uuid4())

            logger.info(f"[Worker] Processing: {payload.source_url} (id={entry_id})")

            log_ingest_entry(entry_id, payload.source_url, payload.type, "processing")

            try:
                # ── YouTube: transcript, metadata, video download ──
                is_youtube = (
                    payload.source_platform == "youtube"
                    or "youtube.com" in payload.source_url
                    or "youtu.be" in payload.source_url
                )

                if is_youtube:
                    video_id = extract_video_id(payload.source_url)
                    if video_id:
                        logger.info(f"[Worker] YouTube video detected: {video_id}")

                        # Fetch transcript and use as content if payload is empty
                        if not payload.content.strip():
                            transcript = await get_youtube_transcript(video_id)
                            if transcript:
                                payload.content = transcript
                                logger.info(f"[Worker] Transcript fetched: {len(transcript)} chars")
                            else:
                                payload.content = f"[No transcript available for this video]\n\nURL: {payload.source_url}"
                                logger.warning(f"[Worker] No transcript for {video_id}")

                        # Enrich metadata with real YouTube data
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

                        # Trigger video download as a fire-and-forget background task
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
                        payload.content = f"[Failed to extract content from {payload.source_url}]\n\nThe page could not be parsed. The URL has been saved for reference."
                        logger.warning(f"[Worker] Article extraction failed and no selection available: {payload.source_url}")

                enrichment = await enrich_with_llm(payload)

                # Route: YouTube → transcripts.md, everything else → bookmarks.md
                is_youtube = (
                    payload.source_platform == "youtube"
                    or "youtube.com" in payload.source_url
                    or "youtu.be" in payload.source_url
                )

                if is_youtube:
                    content_header = "Transcript" if payload.content and "[No transcript" not in payload.content else "Content"
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

                entry = format_bookmark_entry(
                    payload=payload,
                    entry_id=entry_id,
                    status="failed",
                )

                is_youtube = (
                    payload.source_platform == "youtube"
                    or "youtube.com" in payload.source_url
                    or "youtu.be" in payload.source_url
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
            logger.error(f"[Worker] Unexpected error: {e}")
            await asyncio.sleep(1)


# ──────────────────────────────────────────────
# API Endpoints
# ──────────────────────────────────────────────


@app.on_event("startup")
async def startup():
    """Initialize the ingest log and start the background worker."""
    init_ingest_log()
    asyncio.create_task(background_worker())
    logger.info("[Server] Orbit V2 ingestion server started.")


@app.post("/ingest", status_code=202)
async def ingest(payload: IngestPayload):
    """
    Accept a normalized bookmark payload and queue it for async processing.

    Returns 202 Accepted immediately — processing happens in the background.
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
