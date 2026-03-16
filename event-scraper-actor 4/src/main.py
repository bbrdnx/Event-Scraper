"""
Event Scraper Actor for Apify
Reads pending URLs from Notion, scrapes event data, uses OCR + Claude AI
to extract structured event information, and updates the Notion database.
"""

import asyncio
import base64
import json
import re
from datetime import datetime
from urllib.parse import urlparse

import httpx
from apify import Actor


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


async def get_pending_entries(client, database_id, api_key):
    """Fetch all entries with Scrape Status = Pending from Notion."""
    body = {
        "filter": {
            "property": "Scrape Status",
            "select": {"equals": "Pending"},
        }
    }
    result = await notion_request(
        client, "POST", f"/databases/{database_id}/query", api_key, body
    )
    return result.get("results", [])


async def update_notion_entry(client, page_id, api_key, properties):
    """Update a Notion page with extracted event data."""
    body = {"properties": properties}
    await notion_request(client, "PATCH", f"/pages/{page_id}", api_key, body)


async def create_notion_entry(client, database_id, api_key, properties):
    """Create a new Notion page in the database."""
    body = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }
    return await notion_request(client, "POST", "/pages", api_key, body)


def build_notion_properties(data):
    """Convert extracted event data into Notion property format."""
    props = {}

    if data.get("event_name"):
        props["Event Name"] = {
            "title": [{"text": {"content": data["event_name"][:200]}}]
        }
    if data.get("event_location"):
        props["Event Location"] = {
            "rich_text": [{"text": {"content": data["event_location"][:200]}}]
        }
    if data.get("city"):
        props["City"] = {
            "rich_text": [{"text": {"content": data["city"][:100]}}]
        }
    if data.get("state"):
        props["State/Province"] = {
            "rich_text": [{"text": {"content": data["state"][:100]}}]
        }
    if data.get("country"):
        props["Country"] = {
            "rich_text": [{"text": {"content": data["country"][:100]}}]
        }
    if data.get("region"):
        props["Region"] = {"select": {"name": data["region"]}}
    if data.get("event_date"):
        props["Event Date"] = {"date": {"start": data["event_date"]}}
    if data.get("event_time"):
        props["Event Time"] = {
            "rich_text": [{"text": {"content": data["event_time"][:100]}}]
        }
    if data.get("description"):
        props["Description"] = {
            "rich_text": [{"text": {"content": data["description"][:2000]}}]
        }
    if data.get("image_url"):
        props["Image URL"] = {"url": data["image_url"]}
    if data.get("host"):
        props["Host"] = {
            "rich_text": [{"text": {"content": data["host"][:200]}}]
        }
    if data.get("host_handle"):
        props["Host Handle"] = {
            "rich_text": [{"text": {"content": data["host_handle"][:100]}}]
        }
    if data.get("host_platform"):
        props["Host Platform"] = {"select": {"name": data["host_platform"]}}
    if data.get("event_category"):
        props["Event Category"] = {
            "multi_select": [{"name": data["event_category"]}]
        }

    # Determine data quality
    missing = []
    if not data.get("city") or not data.get("country"):
        missing.append("Missing Location")
    if not data.get("event_date"):
        missing.append("Missing Date")

    if missing:
        props["Data Quality"] = {"select": {"name": missing[0]}}
    else:
        props["Data Quality"] = {"select": {"name": "Complete"}}

    props["Scrape Status"] = {"select": {"name": "Scraped"}}

    return props


# ── Platform detection ──────────────────────────────────────────────────────

def detect_platform(url):
    """Detect whether a URL is from Instagram, TikTok, or a website."""
    domain = urlparse(url).netloc.lower()
    if "instagram.com" in domain or "instagr.am" in domain:
        return "Instagram"
    elif "tiktok.com" in domain:
        return "TikTok"
    else:
        return "Website"


# ── Scraping functions ──────────────────────────────────────────────────────

async def scrape_instagram(client, url, apify_token):
    """Scrape Instagram post data using Apify's Instagram Scraper."""
    try:
        # Clean the URL — remove tracking params that cause issues
        clean_url = url.split("?")[0] + "/" if not url.split("?")[0].endswith("/") else url.split("?")[0]

        resp = await client.post(
            "https://api.apify.com/v2/acts/apify~instagram-scraper/run-sync-get-dataset-items",
            params={"token": apify_token},
            json={
                "directUrls": [clean_url],
                "resultsLimit": 1,
            },
            timeout=120,
        )
        if resp.status_code in (200, 201):
            items = resp.json()
            if items and len(items) > 0:
                item = items[0]
                
                # Check if it's an error-only response
                if item.get("error") and not item.get("caption"):
                    Actor.log.warning(f"Instagram returned error: {item.get('error')} - {item.get('errorDescription', '')}")
                else:
                    caption = item.get("caption", "") or ""
                    post_type = item.get("type", "").lower()

                    # Get image/thumbnail URL based on post type
                    image_url = ""

                    # 1. Direct display URL (works for Image and Sidecar posts)
                    if item.get("displayUrl"):
                        image_url = item["displayUrl"]

                    # 2. For video/reel posts, grab the thumbnail
                    if not image_url and item.get("videoUrl"):
                        # Videos have a thumbnail in displayUrl usually,
                        # but if not, check other fields
                        if item.get("previewUrl"):
                            image_url = item["previewUrl"]

                    # 3. Check images array (carousel/sidecar posts)
                    if not image_url and item.get("images") and len(item["images"]) > 0:
                        image_url = item["images"][0]

                    # 4. Check childPosts for carousel first image
                    if not image_url and item.get("childPosts") and len(item["childPosts"]) > 0:
                        first_child = item["childPosts"][0]
                        image_url = first_child.get("displayUrl", "") or first_child.get("imageUrl", "")

                    # Get owner info
                    owner_username = item.get("ownerUsername", "") or ""

                    Actor.log.info(f"Instagram scraper got caption: {len(caption)} chars")
                    Actor.log.info(f"Instagram scraper got image: {'yes' if image_url else 'no'}")
                    Actor.log.info(f"Instagram post type: {post_type}")
                    return {
                        "caption": caption,
                        "image_url": image_url,
                        "author": owner_username,
                    }
        else:
            # Log the actual error for debugging
            try:
                error_body = resp.json()
                Actor.log.warning(f"Instagram scraper status {resp.status_code}: {error_body}")
            except Exception:
                Actor.log.warning(f"Instagram scraper returned status {resp.status_code}")
    except Exception as e:
        Actor.log.warning(f"Instagram Apify scraper failed: {e}")

    # Fallback: try oEmbed
    try:
        oembed_url = f"https://api.instagram.com/oembed/?url={url}"
        resp = await client.get(oembed_url, follow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "caption": data.get("title", ""),
                "image_url": data.get("thumbnail_url", ""),
                "author": data.get("author_name", ""),
            }
    except Exception:
        pass

    return {"caption": "", "image_url": "", "author": ""}


async def scrape_tiktok(client, url, apify_token):
    """Scrape TikTok post data using Apify's TikTok Scraper."""
    try:
        # Call clockworks' TikTok Scraper
        resp = await client.post(
            "https://api.apify.com/v2/acts/clockworks~free-tiktok-scraper/run-sync-get-dataset-items",
            params={"token": apify_token},
            json={
                "postURLs": [url],
                "resultsPerPage": 1,
            },
            timeout=120,
        )
        if resp.status_code in (200, 201):
            items = resp.json()
            if items and len(items) > 0:
                item = items[0]
                caption = item.get("text", "") or item.get("desc", "") or ""
                image_url = ""

                # TikTok videos always have cover/thumbnail images
                if item.get("videoMeta", {}).get("coverUrl", ""):
                    image_url = item["videoMeta"]["coverUrl"]
                elif item.get("covers", {}).get("default", ""):
                    image_url = item["covers"]["default"]
                elif item.get("cover", ""):
                    image_url = item["cover"]

                Actor.log.info(f"TikTok scraper got caption: {len(caption)} chars")
                Actor.log.info(f"TikTok scraper got image: {'yes' if image_url else 'no'}")
                return {
                    "caption": caption,
                    "image_url": image_url,
                    "author": item.get("authorMeta", {}).get("name", ""),
                }

        Actor.log.warning(f"TikTok scraper returned status {resp.status_code}")
    except Exception as e:
        Actor.log.warning(f"TikTok Apify scraper failed: {e}")

    # Fallback: try oEmbed
    try:
        oembed_url = f"https://www.tiktok.com/oembed?url={url}"
        resp = await client.get(oembed_url, follow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "caption": data.get("title", ""),
                "image_url": data.get("thumbnail_url", ""),
                "author": data.get("author_name", ""),
            }
    except Exception:
        pass

    return {"caption": "", "image_url": "", "author": ""}


async def scrape_website(client, url):
    """Scrape generic website for event information."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = await client.get(url, headers=headers, follow_redirects=True, timeout=30)
        text = resp.text

        # Extract useful meta/content
        title = ""
        description = ""
        image_url = ""

        title_match = re.search(r"<title>([^<]*)</title>", text, re.IGNORECASE)
        og_desc = re.search(r'<meta property="og:description" content="([^"]*)"', text)
        meta_desc = re.search(r'<meta name="description" content="([^"]*)"', text)
        og_image = re.search(r'<meta property="og:image" content="([^"]*)"', text)

        if title_match:
            title = title_match.group(1).strip()
        if og_desc:
            description = og_desc.group(1)
        elif meta_desc:
            description = meta_desc.group(1)
        if og_image:
            image_url = og_image.group(1)

        # Also grab visible text (simplified)
        body_text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
        body_text = re.sub(r"<style[^>]*>.*?</style>", "", body_text, flags=re.DOTALL)
        body_text = re.sub(r"<[^>]+>", " ", body_text)
        body_text = re.sub(r"\s+", " ", body_text).strip()[:3000]

        return {
            "caption": f"{title}\n{description}\n{body_text}",
            "image_url": image_url,
            "author": "",
        }
    except Exception as e:
        Actor.log.warning(f"Website scrape failed: {e}")
        return {"caption": "", "image_url": "", "author": ""}


# ── OCR via Claude Vision ───────────────────────────────────────────────────

async def ocr_image(client, image_url, claude_api_key):
    """Use Claude's vision capability to extract text from an image."""
    if not image_url:
        return ""

    try:
        # Download the image
        img_resp = await client.get(image_url, follow_redirects=True, timeout=30)
        if img_resp.status_code != 200:
            return ""

        content_type = img_resp.headers.get("content-type", "image/jpeg")
        if "png" in content_type:
            media_type = "image/png"
        elif "gif" in content_type:
            media_type = "image/gif"
        elif "webp" in content_type:
            media_type = "image/webp"
        else:
            media_type = "image/jpeg"

        img_b64 = base64.b64encode(img_resp.content).decode("utf-8")

        # Send to Claude for OCR
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": claude_api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": img_b64,
                                },
                            },
                            {
                                "type": "text",
                                "text": (
                                    "Extract ALL text visible in this image. "
                                    "Focus on event details: event name, date, time, "
                                    "location, venue, city, address. "
                                    "Return only the extracted text, no commentary."
                                ),
                            },
                        ],
                    }
                ],
            },
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data["content"][0]["text"]
    except Exception as e:
        Actor.log.warning(f"OCR failed: {e}")

    return ""


# ── AI extraction via Claude ───────────────────────────────────────────────

EXTRACTION_PROMPT = """You are an event data extractor. Given the following text from a social media post or website (combining caption text and OCR from images), extract structured event information.

USER NOTES (HIGHEST PRIORITY — these come directly from the person who saved this event and override any conflicting scraped data):
{user_notes}

CAPTION/PAGE TEXT:
{caption}

OCR TEXT FROM IMAGE:
{ocr_text}

SOURCE URL:
{url}

IMPORTANT: A single post may contain MULTIPLE events. This happens when:
- A post lists several different events (e.g. a weekend lineup, a series of shows)
- A post mentions a date range where each day has a distinct event or performer
- A flyer shows multiple acts on different dates

If you find multiple distinct events, return one object per event. If it's a single multi-day event (same name, same venue, date range), return ONE object with the start date.

Extract the following fields for each event. If a field cannot be determined, use null.
When User Notes provide information, ALWAYS use that over scraped data — the user knows details that may not be in the post.
For event_date, use ISO format (YYYY-MM-DD). If you see a year is missing, assume the nearest upcoming occurrence.
For region, classify into one of: Southwest US, West Coast, Southeast US, Northeast US, Midwest, Pacific Northwest, International.
For event_category, classify into one of: Music, Festival, Market, Art, Nightlife, Community, Food & Drink, Sports, Workshop, Family, Wellness, Adult, Other.
For host_platform, classify into one of: Instagram, TikTok, Website, Facebook, Twitter/X, Threads, Bluesky. Infer this from the source URL or from any social handles mentioned in the caption.
For host_handle, extract the social media handle WITHOUT the @ symbol.
Today's date for reference: {today}

Respond ONLY with a valid JSON array, no markdown backticks, no explanation:
[
    {{
        "event_name": "string or null",
        "event_location": "venue name or null",
        "city": "string or null",
        "state": "state/province or null",
        "country": "string or null",
        "region": "one of the region options or null",
        "event_date": "YYYY-MM-DD or null",
        "event_time": "string like '7:00 PM' or 'Doors at 6, Show at 7' or null",
        "description": "brief description of the event, max 500 chars",
        "host": "name of the organizing group or person, or null",
        "host_handle": "social media handle without @ symbol, or null",
        "host_platform": "one of the platform options or null",
        "event_category": "one of the category options or null"
    }}
]

Always return an array, even if there is only one event."""


async def extract_event_data(client, caption, ocr_text, url, claude_api_key, user_notes=""):
    """Use Claude to extract structured event data from text. Returns a list of events."""
    today = datetime.now().strftime("%Y-%m-%d")
    prompt = EXTRACTION_PROMPT.format(
        caption=caption or "(no caption)",
        ocr_text=ocr_text or "(no OCR text)",
        url=url,
        user_notes=user_notes or "(no user notes)",
        today=today,
    )

    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": claude_api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            text = data["content"][0]["text"].strip()
            # Clean potential markdown fences
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            parsed = json.loads(text)

            # Normalize: always return a list
            if isinstance(parsed, dict):
                return [parsed]
            elif isinstance(parsed, list):
                return parsed
            else:
                return []
    except Exception as e:
        Actor.log.error(f"Claude extraction failed: {e}")

    return []


# ── Main actor logic ────────────────────────────────────────────────────────

async def main():
    async with Actor:
        # Get input configuration
        actor_input = await Actor.get_input() or {}
        notion_api_key = actor_input.get("notion_api_key")
        notion_database_id = actor_input.get("notion_database_id")
        claude_api_key = actor_input.get("claude_api_key")
        apify_token = actor_input.get("apify_token")

        if not all([notion_api_key, notion_database_id, claude_api_key, apify_token]):
            Actor.log.error(
                "Missing required input: notion_api_key, notion_database_id, "
                "claude_api_key, or apify_token"
            )
            await Actor.fail()
            return

        async with httpx.AsyncClient(timeout=60) as client:
            # 1. Fetch pending entries from Notion
            Actor.log.info("Fetching pending entries from Notion...")
            entries = await get_pending_entries(
                client, notion_database_id, notion_api_key
            )
            Actor.log.info(f"Found {len(entries)} pending entries")

            if not entries:
                Actor.log.info("No pending entries to process. Done!")
                return

            # 2. Process each entry
            success_count = 0
            fail_count = 0

            for entry in entries:
                page_id = entry["id"]

                try:
                    # Get the source URL from the entry
                    url_prop = entry.get("properties", {}).get("Source URL", {})
                    source_url = url_prop.get("url", "")

                    # Get user notes if any
                    notes_prop = entry.get("properties", {}).get("User Notes", {})
                    notes_rt = notes_prop.get("rich_text", [])
                    user_notes = notes_rt[0]["text"]["content"] if notes_rt else ""
                    if user_notes:
                        Actor.log.info(f"User notes found: {user_notes[:100]}...")

                    if not source_url:
                        Actor.log.warning(f"No URL found for page {page_id}, skipping")
                        await update_notion_entry(
                            client,
                            page_id,
                            notion_api_key,
                            {"Scrape Status": {"select": {"name": "Failed"}}},
                        )
                        fail_count += 1
                        continue

                    # Fix doubled URLs (iOS Shortcut bug)
                    halfway = len(source_url) // 2
                    if (
                        len(source_url) > 20
                        and source_url[:halfway] == source_url[halfway:]
                    ):
                        source_url = source_url[:halfway]
                        Actor.log.info(f"Fixed doubled URL: {source_url}")

                    Actor.log.info(f"Processing: {source_url}")

                    # 3. Detect platform and scrape
                    platform = detect_platform(source_url)
                    Actor.log.info(f"Platform detected: {platform}")

                    if platform == "Instagram":
                        scraped = await scrape_instagram(client, source_url, apify_token)
                    elif platform == "TikTok":
                        scraped = await scrape_tiktok(client, source_url, apify_token)
                    else:
                        scraped = await scrape_website(client, source_url)

                    # 4. OCR on the image if available
                    ocr_text = ""
                    if scraped.get("image_url"):
                        Actor.log.info("Running OCR on image...")
                        ocr_text = await ocr_image(
                            client, scraped["image_url"], claude_api_key
                        )
                        Actor.log.info(
                            f"OCR extracted {len(ocr_text)} characters"
                        )

                    # 5. AI extraction
                    Actor.log.info("Extracting event data with Claude...")
                    events_list = await extract_event_data(
                        client,
                        scraped.get("caption", ""),
                        ocr_text,
                        source_url,
                        claude_api_key,
                        user_notes=user_notes,
                    )

                    if not events_list:
                        Actor.log.warning(
                            f"Could not extract event data for {source_url}"
                        )
                        await update_notion_entry(
                            client,
                            page_id,
                            notion_api_key,
                            {
                                "Scrape Status": {"select": {"name": "Failed"}},
                            },
                        )
                        fail_count += 1
                        continue

                    Actor.log.info(
                        f"Found {len(events_list)} event(s) in this entry"
                    )

                    # Process each event found
                    for i, event_data in enumerate(events_list):
                        # Add image URL to the data
                        event_data["image_url"] = scraped.get("image_url", "")

                        # Build Notion properties
                        properties = build_notion_properties(event_data)
                        properties["Platform"] = {"select": {"name": platform}}
                        properties["Source URL"] = {"url": source_url}

                        if i == 0:
                            # First event: update the original row
                            await update_notion_entry(
                                client, page_id, notion_api_key, properties
                            )
                            Actor.log.info(
                                f"Updated original row: "
                                f"{event_data.get('event_name', 'Unknown Event')}"
                            )
                        else:
                            # Additional events: create new rows
                            await create_notion_entry(
                                client,
                                notion_database_id,
                                notion_api_key,
                                properties,
                            )
                            Actor.log.info(
                                f"Created new row for: "
                                f"{event_data.get('event_name', 'Unknown Event')}"
                            )

                        # Store result in dataset for logging
                        await Actor.push_data(
                            {
                                "url": source_url,
                                "platform": platform,
                                "event_name": event_data.get("event_name"),
                                "city": event_data.get("city"),
                                "event_date": event_data.get("event_date"),
                                "status": "success",
                                "event_index": i + 1,
                                "total_events": len(events_list),
                            }
                        )

                    Actor.log.info(
                        f"Successfully processed {len(events_list)} event(s) "
                        f"from: {source_url}"
                    )
                    success_count += 1

                except Exception as e:
                    Actor.log.error(
                        f"Error processing page {page_id}: {e}"
                    )
                    try:
                        await update_notion_entry(
                            client,
                            page_id,
                            notion_api_key,
                            {
                                "Scrape Status": {"select": {"name": "Failed"}},
                            },
                        )
                    except Exception:
                        Actor.log.error(
                            f"Could not update failure status for {page_id}"
                        )
                    fail_count += 1
                    continue

            Actor.log.info(
                f"Finished: {success_count} succeeded, {fail_count} failed "
                f"out of {len(entries)} entries"
            )
