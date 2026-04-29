// background.js — Orbit V2 Service Worker
// Receives normalized payloads from the content script bridge,
// POSTs them to the local FastAPI server, and manages the offline queue.

const ORBIT_SERVER = "http://localhost:8000";
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

// ──────────────────────────────────────────────
// 1. EXTENSION LIFECYCLE
// ──────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(() => {
  console.log("[Orbit] V2 installed — fetch interception active.");
  setupContextMenus();
  restoreOfflineQueue();
});

chrome.runtime.onStartup.addListener(() => {
  restoreOfflineQueue();
});

// ──────────────────────────────────────────────
// 2. CONTEXT MENUS (YouTube, Articles, Any Page)
// ──────────────────────────────────────────────

function setupContextMenus() {
  chrome.contextMenus.removeAll(() => {
    chrome.contextMenus.create({
      id: "orbit-save-page",
      title: "Save to Orbit",
      contexts: ["page", "selection", "link"],
    });
  });
}

chrome.contextMenus.onClicked.addListener(async (info, tab) => {
  if (info.menuItemId !== "orbit-save-page") return;

  const payload = {
    type: "article",
    source_url: info.linkUrl || tab.url,
    source_platform: extractPlatform(info.linkUrl || tab.url),
    author: "",
    author_handle: "",
    title: tab.title || "",
    captured_at: new Date().toISOString(),
    published_at: null,
    content: "",
    selection: info.selectionText || null,
  };

  await sendToServer(payload);
});

// ──────────────────────────────────────────────
// 3. MESSAGE LISTENER — From Content Script
// ──────────────────────────────────────────────
// The content script bridge forwards intercepted
// bookmark payloads from the page's main world.

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.type === "BOOKMARK_CAPTURED" && request.payload) {
    console.log("[Orbit] Bookmark captured:", request.payload.source_url);
    sendToServer(request.payload).then(() => {
      sendResponse({ status: "queued" });
    });
    return true; // Keep message channel open for async response
  }
  return false;
});

// ──────────────────────────────────────────────
// 4. PLATFORM DETECTOR
// ──────────────────────────────────────────────

function extractPlatform(url) {
  if (!url) return "other";
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    if (hostname.includes("youtube")) return "youtube";
    if (hostname.includes("x.com") || hostname.includes("twitter")) return "x";
    if (hostname.includes("medium")) return "medium";
    if (hostname.includes("substack")) return "substack";
    return "other";
  } catch {
    return "other";
  }
}

// ──────────────────────────────────────────────
// 5. SERVER COMMUNICATION
// ──────────────────────────────────────────────
// Sends normalized payload to the local FastAPI
// server. If the server is down, queues the
// payload in chrome.storage.local for retry.

async function sendToServer(payload) {
  console.log("[Orbit] Sending to server:", payload.type, payload.source_url);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(`${ORBIT_SERVER}/ingest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(5000),
      });

      if (response.ok) {
        console.log("[Orbit] Ingested successfully:", payload.source_url);
        return;
      }
    } catch (err) {
      console.warn(
        `[Orbit] Attempt ${attempt}/${MAX_RETRIES} failed:`,
        err.message
      );
    }

    if (attempt < MAX_RETRIES) {
      await sleep(RETRY_DELAY_MS * attempt);
    }
  }

  // All retries exhausted — queue for offline retry
  console.warn("[Orbit] Server unreachable. Queuing payload locally.");
  await queuePayload(payload);
}

// ──────────────────────────────────────────────
// 6. OFFLINE QUEUE
// ──────────────────────────────────────────────
// Stores failed payloads in chrome.storage.local.
// Restored on extension startup or when the
// server becomes reachable again.

async function queuePayload(payload) {
  const { queue = [] } = await chrome.storage.local.get("queue");
  queue.push({ payload, queued_at: new Date().toISOString() });
  await chrome.storage.local.set({ queue });
  console.log(`[Orbit] Queued. Queue size: ${queue.length}`);
}

async function restoreOfflineQueue() {
  const { queue = [] } = await chrome.storage.local.get("queue");
  if (queue.length === 0) return;

  console.log(`[Orbit] Restoring ${queue.length} queued payloads.`);
  const stillQueued = [];

  for (const item of queue) {
    try {
      const response = await fetch(`${ORBIT_SERVER}/ingest`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(item.payload),
        signal: AbortSignal.timeout(5000),
      });

      if (response.ok) {
        console.log("[Orbit] Flushed queued item:", item.payload.source_url);
      } else {
        stillQueued.push(item);
      }
    } catch {
      stillQueued.push(item);
    }
  }

  await chrome.storage.local.set({ queue: stillQueued });
  console.log(`[Orbit] Queue remaining: ${stillQueued.length}`);
}

// ──────────────────────────────────────────────
// 7. UTILITIES
// ──────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
