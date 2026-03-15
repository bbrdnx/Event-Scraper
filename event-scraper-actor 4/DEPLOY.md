# Event Scraper Actor — Deployment Guide

## Project Structure

```
event-scraper-actor/
├── .actor/
│   ├── actor.json          # Actor configuration
│   └── input_schema.json   # Input form definition
├── src/
│   ├── __init__.py
│   ├── __main__.py         # Entry point
│   └── main.py             # Main actor logic
├── Dockerfile
└── requirements.txt
```

## How It Works

1. Reads your Notion Event Tracker for entries with Scrape Status = "Pending"
2. Detects the platform (Instagram, TikTok, or Website) from the URL
3. Scrapes the post/page for caption text and image URL
4. Runs OCR on the image using Claude's vision capability
5. Sends combined text to Claude to extract structured event data
6. Updates the Notion entry with all extracted fields
7. Sets Data Quality flag based on completeness

## Deploy to Apify

### Option A: Using Apify Console (Recommended for first time)

1. Go to https://console.apify.com
2. Click **Actors** in the left sidebar
3. Click **Create new** → **Create from scratch**
4. Name it: `event-scraper`
5. In the **Source** tab, select **Web IDE**
6. Delete any existing files
7. Create the file structure by uploading or pasting each file:
   - Create folder `.actor/` → add `actor.json` and `input_schema.json`
   - Create folder `src/` → add `__init__.py`, `__main__.py`, and `main.py`
   - Add `Dockerfile` and `requirements.txt` at root level
8. Click **Build** (bottom right)
9. Wait for the build to complete (takes 1-2 minutes)

### Option B: Using Apify CLI

```bash
# Install Apify CLI
npm install -g apify-cli

# Login
apify login

# Navigate to the actor directory
cd event-scraper-actor

# Push to Apify
apify push
```

## Configure the Actor

1. After building, go to the **Input** tab
2. Fill in:
   - **Notion API Key**: your ntn_ key
   - **Notion Database ID**: 3247d9220d3e8098af23eb1f2ffb055b
   - **Claude API Key**: your sk-ant- key
3. Click **Save**

## Set Up Scheduling (7am and 7pm)

1. Go to the **Schedules** section (left sidebar in Apify Console)
2. Click **Create new**
3. Name: `Event Scraper - Morning`
4. Cron expression: `0 7 * * *`
5. Timezone: America/Phoenix
6. Actor: select your `event-scraper`
7. Input: use the saved input configuration
8. Click **Save**

Repeat for the evening run:
- Name: `Event Scraper - Evening`
- Cron expression: `0 19 * * *`

## Manual Run

- Go to your actor page → click **Start** to run manually anytime
- Or use the Apify API:
  ```
  POST https://api.apify.com/v2/acts/YOUR_USERNAME~event-scraper/runs
  ```

## Adding Custom Schedule Times

To add up to 2 extra custom times:
1. Go to **Schedules**
2. Create a new schedule with your desired cron expression
3. Examples:
   - `0 12 * * *` = noon daily
   - `0 9 * * 1` = 9am Mondays only
   - `0 15 * * 1-5` = 3pm weekdays
