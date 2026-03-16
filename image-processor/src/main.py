"""
Image Processor Actor for Apify (Pass 2: Image Upload)
Finds scraped entries without permanent Supabase image URLs,
downloads images, uploads to Supabase storage, and updates Notion.
Falls back to re-scraping if CDN URLs have expired.
"""

import asyncio
import json
import re
import uuid
from datetime import datetime
from urllib.parse import urlparse

import httpx
from apify import Actor

# ── Configuration ───────────────────────────────────────────────────────────

CONCURRENCY = 5  # Image uploads can be more parallel than scraping
SUPABASE_BUCKET = "event-images"


# ── Notion helpers ──────────────────────────────────────────────────────────

async def notion_request(client, method, endpoint, api_key, body=None):
    """Make a request to the Notion API."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    url = f"https://api.notion.com/v1{endpoint}"
    if method == "GET":
        resp = await client.get(url, headers=headers)
    elif method == "POST":
        resp = await client.post(url, headers=headers, json=body)
    elif method == "PATCH":
        resp = await client.patch(url, headers=headers, json=body)
    if resp.status_code >= 400:
        try:
            error_data = resp.json()
            Actor.log.error(f"Notion API error: {error_data.get('message', resp.text)}")
        except Exception:
            Actor.log.error(f"Notion API error: {resp.text}")
    resp.raise_for_status()
    return resp.json()


async def get_entries_needing_images(client, database_id, api_key):
    """Fetch entries that are Scraped but need permanent image URLs.
    Finds rows where Scrape Status = Scraped AND Image URL contains
    instagram CDN or tiktok CDN domains (not yet on Supabase)."""
    # Get all scraped entries — we'll filter client-side for CDN URLs
    body = {
        "filter": {
            "property": "Scrape Status",
            "select": {"equals": "Scraped"},
        }
    }
    result = await notion_request(
        client, "POST", f"/databases/{database_id}/query", api_key, body
    )
    entries = result.get("results", [])

    # Filter to entries with CDN URLs (not Supabase) or empty image URLs
    needs_upload = []
    for entry in entries:
        image_prop = entry.get("properties", {}).get("Image URL", {})
        image_url = image_prop.get("url", "") or ""

        if not image_url:
            # No image at all — needs re-scrape
            needs_upload.append(entry)
        elif "supabase" not in image_url.lower():
            # Has a non-Supabase URL (CDN) — needs upload
            needs_upload.append(entry)

    return needs_upload


async def update_notion_image(client, page_id, api_key, image_url):
    """Update just the Image URL field on a Notion page."""
    body = {
        "properties": {
            "Image URL": {"url": image_url},
        }
    }
    await notion_request(client, "PATCH", f"/pages/{page_id}", api_key, body)


# ── Platform detection ──────────────────────────────────────────────────────

def detect_platform(url):
    """Detect platform from URL."""
    domain = urlparse(url).netloc.lower()
    if "instagram.com" in domain or "instagr.am" in domain:
        return "instagram"
    elif "tiktok.com" in domain:
        return "tiktok"
    else:
        return "website"


# ── Re-scrape for fresh image URL ──────────────────────────────────────────

async def rescrape_image_url(client, source_url, apify_token):
    """Re-scrape the source URL to get a fresh CDN image URL."""
    platform = detect_platform(source_url)

    if platform == "instagram":
        try:
            clean_url = source_url.split("?")[0]
            if not clean_url.endswith("/"):
                clean_url += "/"
            resp = await client.post(
                "https://api.apify.com/v2/acts/apify~instagram-scraper/run-sync-get-dataset-items",
                params={"token": apify_token},
                json={"directUrls": [clean_url], "resultsLimit": 1},
                timeout=120,
            )
            if resp.status_code in (200, 201):
                items = resp.json()
                if items and len(items) > 0:
                    item = items[0]
                    if item.get("displayUrl"):
                        return item["displayUrl"]
                    if item.get("images") and len(item["images"]) > 0:
                        return item["images"][0]
                    if item.get("childPosts") and len(item["childPosts"]) > 0:
                        first_child = item["childPosts"][0]
                        return first_child.get("displayUrl", "") or first_child.get("imageUrl", "")
        except Exception as e:
            Actor.log.warning(f"Instagram re-scrape failed: {e}")

    elif platform == "tiktok":
        try:
            resp = await client.post(
                "https://api.apify.com/v2/acts/clockworks~free-tiktok-scraper/run-sync-get-dataset-items",
                params={"token": apify_token},
                json={"postURLs": [source_url], "resultsPerPage": 1},
                timeout=120,
            )
            if resp.status_code in (200, 201):
                items = resp.json()
                if items and len(items) > 0:
                    item = items[0]
                    if item.get("videoMeta", {}).get("coverUrl", ""):
                        return item["videoMeta"]["coverUrl"]
                    if item.get("covers", {}).get("default", ""):
                        return item["covers"]["default"]
        except Exception as e:
            Actor.log.warning(f"TikTok re-scrape failed: {e}")

    elif platform == "website":
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            resp = await client.get(source_url, headers=headers, follow_redirects=True, timeout=30)
            og_image = re.search(r'<meta property="og:image" content="([^"]*)"', resp.text)
            if og_image:
                return og_image.group(1)
        except Exception as e:
            Actor.log.warning(f"Website re-scrape failed: {e}")

    return ""


# ── Supabase upload ────────────────────────────────────────────────────────

async def upload_to_supabase(client, image_url, supabase_url, supabase_key, platform="unknown"):
    """Download image and upload to Supabase. Returns permanent public URL."""
    if not image_url:
        return ""

    try:
        img_resp = await client.get(image_url, follow_redirects=True, timeout=30)
        if img_resp.status_code != 200:
            return ""

        content_type = img_resp.headers.get("content-type", "image/jpeg")
        if "png" in content_type:
            ext, mime = "png", "image/png"
        elif "webp" in content_type:
            ext, mime = "webp", "image/webp"
        elif "gif" in content_type:
            ext, mime = "gif", "image/gif"
        else:
            ext, mime = "jpg", "image/jpeg"

        unique_id = uuid.uuid4().hex[:12]
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"{platform}/{timestamp}_{unique_id}.{ext}"

        upload_url = f"{supabase_url}/storage/v1/object/{SUPABASE_BUCKET}/{filename}"
        upload_resp = await client.post(
            upload_url,
            headers={
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": mime,
                "x-upsert": "true",
            },
            content=img_resp.content,
            timeout=30,
        )

        if upload_resp.status_code in (200, 201):
            public_url = f"{supabase_url}/storage/v1/object/public/{SUPABASE_BUCKET}/{filename}"
            return public_url
        else:
            Actor.log.warning(f"Supabase upload failed: {upload_resp.status_code} - {upload_resp.text[:200]}")
    except Exception as e:
        Actor.log.warning(f"Supabase upload error: {e}")

    return ""


# ── Single entry processor ─────────────────────────────────────────────────

async def process_image(client, entry, notion_api_key, supabase_url, supabase_key, apify_token):
    """Process a single entry's image. Returns (success, page_id)."""
    page_id = entry["id"]

    try:
        # Get current image URL and source URL
        image_prop = entry.get("properties", {}).get("Image URL", {})
        cdn_url = image_prop.get("url", "") or ""

        source_prop = entry.get("properties", {}).get("Source URL", {})
        source_url = source_prop.get("url", "") or ""

        platform = detect_platform(source_url) if source_url else "unknown"

        # Get event name for logging
        name_prop = entry.get("properties", {}).get("Event Name", {})
        name_title = name_prop.get("title", [])
        event_name = name_title[0]["text"]["content"] if name_title else "Unknown"

        Actor.log.info(f"Processing image for: {event_name}")

        # Step 1: Try downloading from the saved CDN URL
        image_url_to_upload = cdn_url
        if cdn_url:
            try:
                test_resp = await client.head(cdn_url, follow_redirects=True, timeout=10)
                if test_resp.status_code != 200:
                    Actor.log.info(f"CDN URL expired for {event_name}, re-scraping...")
                    image_url_to_upload = ""
            except Exception:
                Actor.log.info(f"CDN URL unreachable for {event_name}, re-scraping...")
                image_url_to_upload = ""

        # Step 2: Re-scrape if CDN URL is dead or missing
        if not image_url_to_upload and source_url:
            Actor.log.info(f"Re-scraping image for: {event_name}")
            image_url_to_upload = await rescrape_image_url(client, source_url, apify_token)

        if not image_url_to_upload:
            Actor.log.warning(f"No image available for: {event_name}")
            return False, page_id

        # Step 3: Upload to Supabase
        permanent_url = await upload_to_supabase(
            client, image_url_to_upload, supabase_url, supabase_key, platform
        )

        if permanent_url:
            # Step 4: Update Notion with permanent URL
            await update_notion_image(client, page_id, notion_api_key, permanent_url)
            Actor.log.info(f"Image uploaded for: {event_name}")
            return True, page_id
        else:
            Actor.log.warning(f"Upload failed for: {event_name}")
            return False, page_id

    except Exception as e:
        Actor.log.error(f"Error processing image for page {page_id}: {e}")
        return False, page_id


# ── Main actor logic ────────────────────────────────────────────────────────

async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}
        notion_api_key = actor_input.get("notion_api_key")
        notion_database_id = actor_input.get("notion_database_id")
        apify_token = actor_input.get("apify_token")
        supabase_url = actor_input.get("supabase_url")
        supabase_key = actor_input.get("supabase_key")

        if not all([notion_api_key, notion_database_id, supabase_url, supabase_key]):
            Actor.log.error(
                "Missing required input: notion_api_key, notion_database_id, "
                "supabase_url, or supabase_key"
            )
            await Actor.fail()
            return

        if not apify_token:
            Actor.log.warning(
                "No apify_token provided — re-scraping will not work for expired CDN URLs"
            )

        async with httpx.AsyncClient(timeout=60) as client:
            Actor.log.info("Fetching entries needing images...")
            entries = await get_entries_needing_images(
                client, notion_database_id, notion_api_key
            )
            Actor.log.info(f"Found {len(entries)} entries needing images")

            if not entries:
                Actor.log.info("All images are up to date. Done!")
                return

            # Process in parallel batches
            success_count = 0
            fail_count = 0
            semaphore = asyncio.Semaphore(CONCURRENCY)

            async def process_with_semaphore(entry):
                async with semaphore:
                    return await process_image(
                        client, entry, notion_api_key,
                        supabase_url, supabase_key, apify_token,
                    )

            tasks = [process_with_semaphore(entry) for entry in entries]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    Actor.log.error(f"Unexpected error: {result}")
                    fail_count += 1
                else:
                    success, page_id = result
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1

            Actor.log.info(
                f"Finished: {success_count} uploaded, {fail_count} failed "
                f"out of {len(entries)} entries"
            )
