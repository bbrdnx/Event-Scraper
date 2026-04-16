#!/usr/bin/env python3
"""
Event Scraper — Supabase Version
Reads Pending events from Supabase, scrapes content from Instagram/TikTok/websites,
extracts structured event data using Claude AI, and writes results back to Supabase.

Input fields:
  supabase_url    - Supabase project URL (https://xxx.supabase.co)
  supabase_key         - Supabase service_role key
  claude_api_key       - Anthropic API key (sk-ant-...)
  apify_token          - Apify API token
  r2_account_id        - Cloudflare account ID
  r2_access_key_id     - R2 API access key ID
  r2_secret_access_key - R2 API secret access key
"""

import asyncio
import json
import base64
import time
import threading
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from apify_client import ApifyClient
from apify import Actor
import anthropic


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def make_headers(key):
    return {
        'apikey': key,
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    }


def get_pending_events(supabase_url, headers):
    """Fetch new events that need scraping (Pending status)."""
    resp = requests.get(
        f'{supabase_url}/rest/v1/events',
        headers={**headers, 'Prefer': 'return=representation'},
        params={'scrape_status': 'eq.Pending', 'select': '*', 'order': 'created_at.asc', 'limit': '50'},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_flagged_events(supabase_url, headers, limit=20):
    """Fetch previously failed events for retry, oldest first."""
    resp = requests.get(
        f'{supabase_url}/rest/v1/events',
        headers={**headers, 'Prefer': 'return=representation'},
        params={'scrape_status': 'eq.Flagged', 'select': '*', 'order': 'updated_at.asc', 'limit': str(limit)},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def update_event(event_id, data, supabase_url, headers):
    resp = requests.patch(
        f'{supabase_url}/rest/v1/events',
        headers=headers,
        params={'id': f'eq.{event_id}'},
        json=data,
        timeout=30,
    )
    resp.raise_for_status()


def insert_event(data, supabase_url, headers):
    resp = requests.post(
        f'{supabase_url}/rest/v1/events',
        headers={**headers, 'Prefer': 'return=representation'},
        json=data,
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json()
    return rows[0] if rows else None


def match_or_create_host(event_id, host_name, host_instagram, supabase_url, headers):
    """Call the match_or_create_host Postgres function to link a host to an event."""
    if not host_name:
        return None
    payload = {
        'p_event_id': event_id,
        'p_host_name': host_name,
    }
    if host_instagram:
        payload['p_instagram'] = host_instagram
    resp = requests.post(
        f'{supabase_url}/rest/v1/rpc/match_or_create_host',
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# Host-related fields that Claude may return but should NOT be written to events table
HOST_FIELDS = {'host_name', 'host_handle', 'host_platform', 'host_instagram'}


def strip_host_fields(data):
    """Remove host fields from event data before writing to events table."""
    return {k: v for k, v in data.items() if k not in HOST_FIELDS}


# ---------------------------------------------------------------------------
# URL / platform helpers
# ---------------------------------------------------------------------------

def detect_platform(url):
    if not url:
        return None
    u = url.lower()
    if 'instagram.com' in u:
        return 'Instagram'
    if 'tiktok.com' in u:
        return 'TikTok'
    return 'Website'


def clean_url(url):
    """Fix doubled URLs from iOS Shortcut and strip Instagram tracking params."""
    if not url:
        return url
    url = url.strip()
    # iOS Shortcut sometimes doubles the URL
    if url.count('http') > 1:
        url = url[:url.index('http', 4)]
    # Strip Instagram tracking query params
    if 'instagram.com' in url and '?' in url:
        url = url.split('?')[0]
    return url


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

# Rate limiter for Instagram scraping — ensures at least INSTAGRAM_DELAY seconds
# between calls across all threads to avoid triggering Instagram rate limits.
INSTAGRAM_DELAY = 3  # seconds between Instagram scrape calls
_ig_lock = threading.Lock()
_ig_last_call = 0.0


def _instagram_rate_limit():
    """Block until enough time has passed since the last Instagram scrape call."""
    global _ig_last_call
    with _ig_lock:
        now = time.monotonic()
        wait = INSTAGRAM_DELAY - (now - _ig_last_call)
        if wait > 0:
            time.sleep(wait)
        _ig_last_call = time.monotonic()


def scrape_instagram(apify_token, url, retries=2):
    """Scrape an Instagram post, retrying on failure since the scraper intermittently times out."""
    for attempt in range(retries):
        try:
            _instagram_rate_limit()
            client = ApifyClient(apify_token)
            run = client.actor('apify/instagram-scraper').call(
                run_input={'directUrls': [url], 'resultsType': 'posts', 'resultsLimit': 1},
                timeout_secs=180,
            )
            items = list(client.dataset(run['defaultDatasetId']).iterate_items())
            if items:
                item = items[0]
                return {
                    'text': item.get('caption', ''),
                    'image_url': item.get('displayUrl') or (item.get('images') or [None])[0],
                    'author': item.get('ownerUsername', ''),
                }
            # Empty result — retry if we have attempts left
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)  # 5s, 10s backoff
                print(f'  Instagram scraper returned empty, waiting {wait}s before retry ({attempt + 1}/{retries})...')
                time.sleep(wait)
                continue
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f'  Instagram scrape failed ({e}), waiting {wait}s before retry ({attempt + 1}/{retries})...')
                time.sleep(wait)
                continue
            raise
    return {'text': '', 'image_url': None, 'author': ''}


def scrape_tiktok(apify_token, url, retries=2):
    """Scrape a TikTok post, retrying on failure."""
    for attempt in range(retries):
        try:
            client = ApifyClient(apify_token)
            run = client.actor('clockworks/free-tiktok-scraper').call(
                run_input={'postURLs': [url], 'resultsPerPage': 1},
                timeout_secs=180,
            )
            items = list(client.dataset(run['defaultDatasetId']).iterate_items())
            if items:
                item = items[0]
                return {
                    'text': item.get('text', '') or item.get('desc', ''),
                    'image_url': (item.get('videoMeta') or {}).get('coverUrl'),
                    'author': (item.get('authorMeta') or {}).get('name', ''),
                }
            if attempt < retries - 1:
                print(f'  TikTok scraper returned empty, retrying ({attempt + 1}/{retries})...')
                continue
        except Exception as e:
            if attempt < retries - 1:
                print(f'  TikTok scrape failed ({e}), retrying ({attempt + 1}/{retries})...')
                continue
            raise
    return {'text': '', 'image_url': None, 'author': ''}


def scrape_website(url):
    try:
        resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        resp.raise_for_status()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')
        title = soup.find('title')
        og_desc = soup.find('meta', property='og:description')
        og_image = soup.find('meta', property='og:image')
        body = ' '.join(soup.get_text().split())[:3000]
        return {
            'text': '\n'.join(filter(None, [
                title.text.strip() if title else '',
                og_desc.get('content', '') if og_desc else '',
                body,
            ])),
            'image_url': og_image.get('content') if og_image else None,
            'author': '',
        }
    except Exception as e:
        return {'text': '', 'image_url': None, 'author': '', 'error': str(e)}


# ---------------------------------------------------------------------------
# Claude extraction
# ---------------------------------------------------------------------------

def build_system_prompt():
    today = datetime.now().strftime('%Y-%m-%d')
    current_year = datetime.now().year
    date_context = (
        f"Today's date is {today}. When a post mentions a date without a year "
        f"(e.g. \"March 15\" or \"Saturday April 5th\"), assume the year is "
        f"{current_year} unless the content clearly indicates otherwise. "
        f"Events are almost always upcoming, not in the past.\n\n"
    )
    return (
        "You are an event data extractor for The Lez List, a directory of "
        "LGBTQ+ events curated for Black queer women.\n\n"
        + date_context
        + SYSTEM_PROMPT_BODY
    )


SYSTEM_PROMPT_BODY = """Extract structured event data from the provided content and return a JSON array of event objects.

IMPORTANT: User Notes are the HIGHEST PRIORITY data source. If User Notes specify a date, location, or other detail, use that over anything else.

Each event object must have these fields (use null if unknown):
{
  "event_name": "string",
  "description": "string (max 2000 chars, do not truncate mid-sentence)",
  "start_date": "YYYY-MM-DD or null",
  "end_date": "YYYY-MM-DD or null",
  "start_time": "HH:MM:SS or null (24hr format)",
  "end_time": "HH:MM:SS or null",
  "city": "string or null",
  "state": "string (2-letter US abbreviation) or null",
  "country": "US",
  "venue_name": "string or null",
  "address": "string or null",
  "is_virtual": false,
  "virtual_link": "string or null",
  "price": "string or null (e.g. '$20', 'Free', '$10-$25')",
  "is_free": false,
  "host_name": "string or null (display name of the event host/organizer, preserve their exact capitalization)",
  "host_instagram": "string (Instagram handle without @, if visible in post tags or caption) or null",
  "categories": [],
  "region": "Southwest US|West Coast|Southeast US|Northeast US|Midwest|Pacific Northwest|North America|Caribbean|South America|Europe|Africa|Asia|South Pacific|Australia or null",
  "data_quality": "Complete|Missing Location|Missing Date|Incomplete"
}

Category options (only use these): Activity, Adult, Arts & Culture, Black Pride, Book Club, Community, Dance Party, Day Party, Family, Festival, Food & Drink, Get away, Kickback, Market, Meetup, Open Mic, Other, Outdoors, PRIDE, Singles / Dating, Social, Sports, Teen, Wellness, Workshop

Region guidance:
- Southwest US: AZ, NM, NV, TX, OK
- West Coast: CA, OR, WA
- Southeast US: FL, GA, NC, SC, TN, AL, MS, LA, AR, VA
- Northeast US: NY, NJ, CT, MA, PA, MD, DC, DE, RI, NH, VT, ME
- Midwest: IL, OH, MI, MN, WI, MO, IN, KS, NE, IA, ND, SD
- Pacific Northwest: OR, WA, ID, MT
- North America: Canada, Mexico, and other non-US mainland North American countries
- Caribbean: Jamaica, Puerto Rico, Trinidad, Barbados, Bahamas, and other Caribbean islands
- South America: Brazil, Colombia, Argentina, and other South American countries
- Europe: UK (including London), France, Germany, Netherlands, and other European countries
- Africa: Nigeria (including Lagos), Ghana, South Africa, Kenya, and other African countries
- Asia: Japan, India, Thailand, Philippines, and other Asian countries
- South Pacific: Hawaii, Fiji, New Zealand, and other South Pacific islands
- Australia: Australia

Data quality rules:
- Complete: has event_name + start_date + city
- Missing Location: has name + date but no city
- Missing Date: has name + city but no date
- Incomplete: missing two or more of name/date/city

Multi-event rules:
- Return MULTIPLE events only when a post clearly lists separate events with different dates or venues
- A single multi-day event (same name, same venue) = ONE event with start_date and end_date
- When in doubt, return one event

Return ONLY a valid JSON array. No markdown, no explanation, no code fences."""


PLATFORM_FOLDER = {
    'Instagram': 'instagram',
    'TikTok': 'tiktok',
    'Website': 'website',
}

R2_WORKER_BASE_URL = 'https://lez-list-image-worker.broadnaxux.workers.dev'
R2_BUCKET = 'lez-list-images'


def make_r2_client(account_id, access_key_id, secret_access_key):
    import boto3
    from botocore.config import Config
    return boto3.client(
        's3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version='s3v4'),
        region_name='auto',
    )


def download_image(url):
    """Download an image and return (bytes, content_type). Returns (None, None) on failure."""
    try:
        resp = requests.get(url, timeout=30, headers={'User-Agent': 'Mozilla/5.0'})
        if resp.status_code == 200:
            ct = resp.headers.get('content-type', 'image/jpeg').split(';')[0].strip()
            return resp.content, ct
    except Exception:
        pass
    return None, None


def upload_to_r2(image_bytes, content_type, platform, event_id, r2_client):
    """Upload image bytes to Cloudflare R2 and return the Worker public URL."""
    folder = PLATFORM_FOLDER.get(platform, 'other')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    short_id = str(event_id).replace('-', '')[:8]
    path = f'{folder}/{timestamp}_{short_id}.jpg'
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET,
            Key=path,
            Body=image_bytes,
            ContentType=content_type,
        )
        return f'{R2_WORKER_BASE_URL}/{path}'
    except Exception as e:
        print(f'  ⚠ R2 upload failed: {e}')
        return None


def fetch_image_as_base64(url):
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            content_type = resp.headers.get('content-type', 'image/jpeg').split(';')[0].strip()
            return base64.b64encode(resp.content).decode(), content_type
    except Exception:
        pass
    return None, None


def normalize_multi_event_fields(extracted):
    """
    When Claude extracts multiple events from a single post, some shared fields
    (venue, city, state, region, host_name, host_instagram) often come back
    populated on event 1 but blank on events 2, 3, etc. This is a Claude
    extraction gap — the info is there in the post, it just doesn't get
    repeated for each sub-event.

    This function fills those gaps by propagating any non-null value found
    across the group. It only fills in blanks — it never overwrites a value
    Claude already extracted for a specific event.

    Only runs when there are 2+ events (single events are unaffected).
    """
    if len(extracted) < 2:
        return extracted

    # Fields that are typically shared across all events from one post
    SHARED_FIELDS = [
        'venue_name', 'city', 'state', 'country', 'address',
        'region', 'host_name', 'host_instagram',
    ]

    for field in SHARED_FIELDS:
        # Find the first non-null, non-empty value across all events
        fill_value = None
        for event in extracted:
            val = event.get(field)
            if val:
                fill_value = val
                break
        if fill_value is None:
            continue
        # Fill blanks in all other events
        filled = 0
        for event in extracted:
            if not event.get(field):
                event[field] = fill_value
                filled += 1
        if filled:
            print(f'  → Normalized "{field}" across {filled} sub-event(s): {fill_value!r}')

    return extracted


def normalize_location_fields(extracted):
    """
    Normalize city names and state abbreviations on extracted events.
    - Resolves city aliases (Philly → Philadelphia, NYC → New York City, etc.)
    - Converts full state names to 2-letter abbreviations (California → CA)
    - Trims whitespace
    - Resolves NYC boroughs to "New York City"
    """
    CITY_ALIASES = {
        'philly': 'Philadelphia', 'phl': 'Philadelphia',
        # NYC: only alias abbreviations, NOT boroughs/neighborhoods.
        # Brooklyn, Bronx, Harlem, Queens etc. keep their names.
        'nyc': 'New York City', 'new york': 'New York City',
        'la': 'Los Angeles', 'l.a.': 'Los Angeles', 'l.a': 'Los Angeles',
        'sf': 'San Francisco', 'san fran': 'San Francisco', 'frisco': 'San Francisco',
        'atl': 'Atlanta', 'the a': 'Atlanta',
        'dc': 'Washington', 'd.c.': 'Washington', 'd.c': 'Washington',
        'washington dc': 'Washington', 'washington d.c.': 'Washington',
        'washington, dc': 'Washington', 'washington, d.c.': 'Washington',
        'htx': 'Houston', 'h-town': 'Houston',
        'chi': 'Chicago', 'chi-town': 'Chicago',
        'nola': 'New Orleans', 'vegas': 'Las Vegas',
        'mia': 'Miami', 'the d': 'Detroit',
        'ft. lauderdale': 'Fort Lauderdale', 'ft lauderdale': 'Fort Lauderdale',
        'ft. worth': 'Fort Worth', 'ft worth': 'Fort Worth',
        'st. louis': 'St. Louis', 'saint louis': 'St. Louis', 'st louis': 'St. Louis',
        'st. petersburg': 'St. Petersburg', 'saint petersburg': 'St. Petersburg',
        'st petersburg': 'St. Petersburg',
    }

    STATE_TO_ABBR = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
        'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
        'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
        'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
        'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
        'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
        'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
        'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
        'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
        'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
        'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
        'wisconsin': 'WI', 'wyoming': 'WY',
        'district of columbia': 'DC', 'puerto rico': 'PR',
        # Canadian provinces
        'ontario': 'ON', 'quebec': 'QC', 'british columbia': 'BC',
        'alberta': 'AB', 'manitoba': 'MB', 'nova scotia': 'NS',
        'new brunswick': 'NB', 'saskatchewan': 'SK',
    }

    for event in extracted:
        # Normalize city
        city = (event.get('city') or '').strip()
        if city:
            alias = CITY_ALIASES.get(city.lower())
            if alias:
                print(f'  → City normalized: {city!r} → {alias!r}')
                city = alias
            event['city'] = city

        # Normalize state
        state = (event.get('state') or '').strip()
        if state:
            upper = state.upper()
            if len(upper) <= 2:
                event['state'] = upper
            else:
                abbr = STATE_TO_ABBR.get(state.lower())
                if abbr:
                    print(f'  → State normalized: {state!r} → {abbr!r}')
                    event['state'] = abbr
                else:
                    event['state'] = state

    return extracted


def validate_extracted_dates(extracted):
    """
    Sanity-check dates returned by Claude.
    Flags any event whose start_date is more than 6 months in the past
    or more than 18 months in the future — almost certainly a year error.
    Returns a list of warning strings (empty if all dates are fine).
    """
    today = datetime.now().date()
    warnings = []
    for event in extracted:
        raw_date = event.get('start_date')
        if not raw_date:
            continue
        try:
            event_date = datetime.strptime(raw_date[:10], '%Y-%m-%d').date()
        except ValueError:
            warnings.append(f"Unparseable date: {raw_date!r}")
            continue
        months_past = (today - event_date).days / 30
        months_future = (event_date - today).days / 30
        if months_past > 6:
            warnings.append(
                f"Date {raw_date} is {int(months_past)} months in the past — possible year error"
            )
        elif months_future > 18:
            warnings.append(
                f"Date {raw_date} is {int(months_future)} months in the future — possible year error"
            )
    return warnings


def extract_with_claude(claude_client, scraped_text, image_url, user_notes):
    user_content = []

    if user_notes:
        user_content.append({'type': 'text', 'text': f'USER NOTES (highest priority):\n{user_notes}\n\n'})

    if scraped_text:
        user_content.append({'type': 'text', 'text': f'SCRAPED TEXT:\n{scraped_text[:4000]}\n\n'})

    # Only send the image to Claude when there's little or no text to extract from.
    # Most events have all the details in the caption text, so sending the flyer image
    # just adds latency and token cost without improving extraction quality.
    has_enough_text = len((scraped_text or '').strip()) > 80 or len((user_notes or '').strip()) > 40
    if image_url and not has_enough_text:
        img_b64, content_type = fetch_image_as_base64(image_url)
        if img_b64:
            user_content.append({
                'type': 'image',
                'source': {'type': 'base64', 'media_type': content_type, 'data': img_b64},
            })
            user_content.append({'type': 'text', 'text': 'Extract any event details visible in this image.'})

    if not user_content:
        return None

    response = claude_client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=2000,
        system=build_system_prompt(),
        messages=[{'role': 'user', 'content': user_content}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    if '```' in raw:
        parts = raw.split('```')
        for p in parts:
            p = p.strip()
            if p.startswith('json'):
                p = p[4:].strip()
            try:
                return json.loads(p)
            except Exception:
                continue
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_event(event, apify_token, claude_client, supabase_url, supabase_key, headers, r2_client, scrape_cache=None):
    event_id = event['id']
    source_url = clean_url(event.get('source_url'))
    image_url = event.get('flyer_url')
    user_notes = event.get('user_notes') or ''

    print(f'Processing {event_id}: {source_url or "(image only)"}')

    platform = detect_platform(source_url)
    scraped = {'text': '', 'image_url': image_url, 'author': ''}

    try:
        if platform:
            update_event(event_id, {'platform': platform}, supabase_url, headers)

        # Use cached scrape result if the same source_url was already scraped this run
        scrape_failed = False
        if scrape_cache is not None and source_url and source_url in scrape_cache:
            scraped = scrape_cache[source_url]
            print(f'  Cache hit for {source_url}')
        elif source_url:
            try:
                if platform == 'Instagram':
                    scraped = scrape_instagram(apify_token, source_url)
                elif platform == 'TikTok':
                    scraped = scrape_tiktok(apify_token, source_url)
                else:
                    scraped = scrape_website(source_url)
                # Store in cache for other events with the same URL
                if scrape_cache is not None:
                    scrape_cache[source_url] = scraped
            except Exception as scrape_err:
                print(f'  ⚠ Scrape failed: {scrape_err}')
                scrape_failed = True

        # Prefer the event's existing flyer_url if scraper found nothing
        if not scraped.get('image_url') and image_url:
            scraped['image_url'] = image_url

        # Even if the scrape failed, try Claude extraction if we have user_notes or an image.
        # User notes alone are often enough to extract event details.
        scraped_text = scraped.get('text', '')
        has_something_to_extract = scraped_text or user_notes or scraped.get('image_url')

        if scrape_failed and not has_something_to_extract:
            # Nothing to work with — mark as Flagged so the next run retries it
            update_event(event_id, {'scrape_status': 'Flagged'}, supabase_url, headers)
            print(f'  ✗ Scrape failed and no fallback data, flagged for retry')
            return

        extracted = extract_with_claude(
            claude_client,
            scraped_text,
            scraped.get('image_url'),
            user_notes,
        )

        if not extracted:
            update_event(event_id, {'scrape_status': 'Flagged'}, supabase_url, headers)
            print(f'  ✗ Claude returned nothing, flagged for retry')
            return

        # Sanity-check extracted dates before writing anything to Supabase.
        # Catches year errors (e.g. Claude returns 2024 when today is 2026).
        # If suspicious dates are found, flag the event for manual review instead
        # of writing bad data — avoids wasted re-scrapes and manual corrections later.
        date_warnings = validate_extracted_dates(extracted)
        if date_warnings:
            for w in date_warnings:
                print(f'  ⚠ Date warning: {w}')
            update_event(event_id, {'scrape_status': 'Flagged'}, supabase_url, headers)
            print(f'  ✗ Suspicious date(s) detected — flagged for manual review')
            return

        # For multi-event posts, fill shared fields (venue, city, region, host, etc.)
        # that Claude populated on event 1 but left blank on siblings.
        extracted = normalize_multi_event_fields(extracted)

        # Normalize location: city aliases (Philly → Philadelphia) and state abbreviations
        extracted = normalize_location_fields(extracted)

        # First event → update the existing row
        first = extracted[0]

        # Extract host info before stripping from event payload
        host_name = first.get('host_name')
        host_instagram = first.get('host_instagram')
        # If Claude didn't find an instagram handle, use the scraped author as fallback
        if scraped.get('author') and not host_instagram and platform in ('Instagram', 'TikTok'):
            host_instagram = scraped['author']

        first['scrape_status'] = 'Scraped'
        # Only set flyer_url if the event doesn't already have a permanent image
        # (Supabase Storage legacy URLs or new R2 Worker URLs)
        existing_flyer = event.get('flyer_url', '') or ''
        has_permanent_image = (
            existing_flyer.startswith(f'{supabase_url}/storage/') or
            existing_flyer.startswith(R2_WORKER_BASE_URL)
        )
        if scraped.get('image_url') and not has_permanent_image:
            # Upload image directly to R2 for a permanent URL
            img_bytes, img_ct = download_image(scraped['image_url'])
            if img_bytes:
                permanent_url = upload_to_r2(
                    img_bytes, img_ct or 'image/jpeg', platform, event_id, r2_client
                )
                if permanent_url:
                    first['flyer_url'] = permanent_url
                    print(f'  ✓ Image uploaded to R2')
                else:
                    first['flyer_url'] = scraped['image_url']  # fall back to CDN URL
            else:
                first['flyer_url'] = scraped['image_url']  # fall back to CDN URL

        # Strip host fields and write event data
        update_event(event_id, strip_host_fields(first), supabase_url, headers)

        # Link host via the host directory
        if host_name:
            try:
                result = match_or_create_host(event_id, host_name, host_instagram, supabase_url, headers)
                if result:
                    print(f'  → Host: {result.get("host_name")} ({result.get("match_type")})')
            except Exception as e:
                print(f'  ⚠ Host matching failed: {e}')

        # Additional events → insert new rows
        for extra in extracted[1:]:
            extra_host_name = extra.get('host_name')
            extra_host_ig = extra.get('host_instagram')

            extra['scrape_status'] = 'Scraped'
            extra['source_url'] = source_url
            extra['platform'] = platform
            if scraped.get('image_url'):
                extra['flyer_url'] = scraped['image_url']  # placeholder for insert

            new_row = insert_event(strip_host_fields(extra), supabase_url, headers)

            # Upload image to R2 for the newly created event
            if new_row and scraped.get('image_url'):
                ex_bytes, ex_ct = download_image(scraped['image_url'])
                if ex_bytes:
                    perm_url = upload_to_r2(
                        ex_bytes, ex_ct or 'image/jpeg', platform, new_row['id'], r2_client
                    )
                    if perm_url:
                        update_event(new_row['id'], {'flyer_url': perm_url}, supabase_url, headers)

            # Link host for the new event
            if new_row and extra_host_name:
                try:
                    match_or_create_host(new_row['id'], extra_host_name, extra_host_ig, supabase_url, headers)
                except Exception as e:
                    print(f'  ⚠ Host matching failed for extra event: {e}')

        print(f'  ✓ {len(extracted)} event(s) extracted')

    except Exception as e:
        print(f'  ✗ Error: {e}')
        try:
            update_event(event_id, {'scrape_status': 'Flagged'}, supabase_url, headers)
        except Exception:
            pass


async def main():
    async with Actor:
        inp = await Actor.get_input() or {}

        supabase_url = inp.get('supabase_url', '').rstrip('/')
        supabase_key = inp.get('supabase_key', '')
        claude_api_key = inp.get('claude_api_key', '')
        apify_token = inp.get('apify_token', '')
        r2_account_id = inp.get('r2_account_id', '')
        r2_access_key_id = inp.get('r2_access_key_id', '')
        r2_secret_access_key = inp.get('r2_secret_access_key', '')

        if not all([supabase_url, supabase_key, claude_api_key, apify_token]):
            raise ValueError('Missing required inputs: supabase_url, supabase_key, claude_api_key, apify_token')
        if not all([r2_account_id, r2_access_key_id, r2_secret_access_key]):
            raise ValueError('Missing required R2 inputs: r2_account_id, r2_access_key_id, r2_secret_access_key')

        headers = make_headers(supabase_key)
        claude_client = anthropic.Anthropic(api_key=claude_api_key)
        r2_client = make_r2_client(r2_account_id, r2_access_key_id, r2_secret_access_key)

        # Cache scraped results by source_url so duplicate URLs aren't re-scraped.
        # ThreadPoolExecutor shares memory, so a plain dict works here — only one
        # thread will scrape a given URL first, and later threads read the cached value.
        scrape_cache = {}

        # --- Pass 1: Fresh Pending events (4 workers) ---
        pending = get_pending_events(supabase_url, headers)
        print(f'Found {len(pending)} pending events')

        if pending:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(process_event, event, apify_token, claude_client, supabase_url, supabase_key, headers, r2_client, scrape_cache)
                    for event in pending
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f'Unhandled error in worker: {e}')

        # --- Pass 2: Flagged retries (2 workers, slower pace) ---
        # Process flagged events separately with lower concurrency to avoid
        # the rate-limiting that likely caused them to fail in the first place.
        flagged = get_flagged_events(supabase_url, headers, limit=20)
        print(f'Found {len(flagged)} flagged events to retry')

        if flagged:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(process_event, event, apify_token, claude_client, supabase_url, supabase_key, headers, r2_client, scrape_cache)
                    for event in flagged
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f'Unhandled error in worker: {e}')

        print('Done.')


if __name__ == '__main__':
    asyncio.run(main())
