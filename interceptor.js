(function () {
  const GRAPHQL_URL = "graphql";
  const TIMELINE_URL = "HomeTimeline";
  const DETAIL_URL = "TweetDetail";
  const BOOKMARK_OPERATION = "CreateBookmark";

  const tweetCache = new Map();
  console.log(`[Orbit] Memory cache initialized. Size: ${tweetCache.size}`);

  const originalFetch = window.fetch;
  window.fetch = async function (input, init) {
    const url = typeof input === "string" ? input : input?.url || "";
    if (!url.includes(GRAPHQL_URL) && !url.includes(TIMELINE_URL) && !url.includes(DETAIL_URL) && !url.includes(BOOKMARK_OPERATION)) {
      return originalFetch.apply(this, arguments);
    }
    if (url.includes(BOOKMARK_OPERATION) || isBookmarkRequest(init)) {
      const tweetId = extractTweetIdFromRequest(init);
      if (tweetId) {
        const cachedTweet = tweetCache.get(tweetId);
        if (cachedTweet) {
          const payload = normalizeTweetPayload(cachedTweet);
          window.postMessage({ source: "orbit-interceptor", type: "bookmark-captured", payload }, "*");
        }
      }
    }
    const response = await originalFetch.apply(this, arguments);
    try {
      const clonedResponse = response.clone();
      const json = await clonedResponse.json();
      const tweetsFound = extractAndCacheTweets(json);
      if (tweetsFound > 0) console.log(`[Orbit] Cached ${tweetsFound} tweet(s)`);
    } catch (err) {}
    return response;
  };

  const originalXHROpen = XMLHttpRequest.prototype.open;
  const originalXHRSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (method, url) {
    this._orbitUrl = url;
    return originalXHROpen.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function (body) {
    const xhr = this;
    const url = xhr._orbitUrl || "";
    if (url.includes(GRAPHQL_URL) || url.includes(TIMELINE_URL) || url.includes(DETAIL_URL) || url.includes(BOOKMARK_OPERATION)) {
      if (url.includes(BOOKMARK_OPERATION)) {
        const tweetId = extractTweetIdFromXhrBody(body);
        if (tweetId) {
          const cachedTweet = tweetCache.get(tweetId);
          if (cachedTweet) {
            const payload = normalizeTweetPayload(cachedTweet);
            window.postMessage({ source: "orbit-interceptor", type: "bookmark-captured", payload }, "*");
          }
        }
      }
      xhr.addEventListener("load", function () {
        try {
          const json = JSON.parse(xhr.responseText);
          const tweetsFound = extractAndCacheTweets(json);
          if (tweetsFound > 0) console.log(`[Orbit] Cached ${tweetsFound} tweet(s) from XHR`);
        } catch (err) {}
      });
    }
    return originalXHRSend.apply(this, arguments);
  };

  function isBookmarkRequest(init) {
    if (!init?.body) return false;
    try {
      const body = typeof init.body === "string" ? JSON.parse(init.body) : init.body;
      const opName = body?.operationName || body?.query || "";
      return opName.includes("Bookmark") || opName.includes("bookmark");
    } catch { return false; }
  }
  
  function extractTweetIdFromRequest(init) {
    if (!init?.body) return null;
    try {
      const body = typeof init.body === "string" ? JSON.parse(init.body) : init.body;
      return body?.variables?.tweet_id || null;
    } catch { return null; }
  }
  
  function extractTweetIdFromXhrBody(body) {
    if (!body) return null;
    try {
      const parsed = typeof body === "string" ? JSON.parse(body) : body;
      return parsed?.variables?.tweet_id || null;
    } catch { return null; }
  }

  function extractAndCacheTweets(json) {
    if (!json?.data) return 0;
    let count = 0;
    const found = findAllTweetObjects(json.data);
    for (const tweet of found) {
      const id = tweet.rest_id || tweet.id_str || tweet.id;
      if (id && tweet.legacy?.full_text) {
        tweet.legacy.rest_id = id;
        tweetCache.set(id, tweet.legacy);
        count++;
      } else if (id && tweet.full_text) {
        tweetCache.set(id, tweet);
        count++;
      }
    }
    return count;
  }

  function findAllTweetObjects(obj, depth = 0) {
    if (!obj || typeof obj !== "object" || depth > 50) return [];
    let results = [];
    if (obj.legacy && obj.legacy.full_text) {
      results.push(obj);
    } else if (obj.full_text && (obj.id_str || obj.rest_id || obj.id)) {
      results.push(obj);
    }
    for (const key of Object.keys(obj)) {
      const val = obj[key];
      if (val && typeof val === "object") {
        results = results.concat(findAllTweetObjects(val, depth + 1));
      }
    }
    return results;
  }

  function normalizeTweetPayload(tweet) {
    const user = tweet.user || tweet.core?.user_results?.result?.legacy || tweet.extended_entities?.user || {};
    const handle = user.screen_name || "unknown";
    const author = user.name || handle;
    return {
      type: "tweet",
      source_url: `https://x.com/${handle}/status/${tweet.rest_id || tweet.id_str || tweet.id}`,
      source_platform: "x",
      author: author,
      author_handle: `@${handle}`,
      title: tweet.full_text?.split("\n")[0]?.slice(0, 100) || "",
      captured_at: new Date().toISOString(),
      published_at: tweet.created_at || null,
      content: tweet.full_text || "",
    };
  }
  
  console.log("[Orbit] Stable Fetch interceptor injected.");
})();