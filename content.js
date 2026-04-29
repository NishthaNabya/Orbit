// content.js — Bridge Script (Isolated World)
// This content script runs in the extension's isolated world on X.com pages.
// Its job is to:
//   1. Inject interceptor.js into the PAGE's main world (via script tag)
//   2. Listen for postMessage events from the interceptor
//   3. Forward normalized payloads to background.js via chrome.runtime.sendMessage

// ──────────────────────────────────────────────
// 1. Inject interceptor.js into the main world
// ──────────────────────────────────────────────
// Content scripts run in an "isolated world" — they can see the DOM but
// cannot access the page's JavaScript (window.fetch, XMLHttpRequest, etc.).
// To monkey-patch those, we must inject a <script> tag that runs in the
// page's own context.

const script = document.createElement("script");
script.src = chrome.runtime.getURL("interceptor.js");
script.onload = function () {
  // Remove the script tag after injection — it's no longer needed.
  // The interceptor's IIFE has already executed and patched the globals.
  this.remove();
};
(document.head || document.documentElement).appendChild(script);

// ──────────────────────────────────────────────
// 2. Listen for postMessage from the interceptor
// ──────────────────────────────────────────────
// The interceptor runs in the page's main world and communicates back
// via window.postMessage. We listen for those messages and forward
// them to the background service worker.

window.addEventListener("message", (event) => {
  // Ignore messages from other sources
  if (!event.data || event.data.source !== "orbit-interceptor") return;
  if (event.data.type !== "bookmark-captured") return;

  const payload = event.data.payload;
  if (!payload) return;

  console.log("[Orbit Bridge] Forwarding payload to background:", payload.source_url);

  chrome.runtime.sendMessage(
    { type: "BOOKMARK_CAPTURED", payload },
    (response) => {
      if (chrome.runtime.lastError) {
        console.warn("[Orbit Bridge] Background script error:", chrome.runtime.lastError.message);
        return;
      }
      console.log("[Orbit Bridge] Background acknowledged:", response);
    }
  );
});

console.log("[Orbit] Content script bridge loaded on:", window.location.hostname);
