import requests

import re
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

# URL of the page where the table lives (we scrape this for the Nonce)
PAGE_URL = os.getenv("SOURCE_PAGE_URL") 
API_URL = os.getenv("SOURCE_API_URL")


def get_nonce_and_config():
    """Fetches the webpage and extracts the EV_HR configuration object."""
    print(f"visiting {PAGE_URL} to get security token...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(PAGE_URL, headers=headers)
    resp.raise_for_status()
    
    # regex to find: var config = { ... };
    # We look for something starting with var EV_HR = and ending with };
    match = re.search(r'var EV_HR\s*=\s*({.*?});', resp.text)
    if not match:
        raise Exception("Could not find config on the page. Website structure might have changed.")
    
    config_str = match.group(1)
    try:
        config = json.loads(config_str)
        print(f"Found Nonce: {config.get('nonce')}")
        return config
    except:
        # Fallback if json parse fails (sometimes keys aren't quoted in simple JS objects)
        # We manually extract the nonce string
        nonce_match = re.search(r'"nonce":"([^"]+)"', config_str)
        if nonce_match:
            return {"nonce": nonce_match.group(1)}
        raise Exception("Failed to parse EV_HR JSON config.")

def process_row(r):
    def clean(val): return str(val) if val is not None else ""

    stake = float(r.get("stake") or 0)
    val_ratio = float(r.get("value_ratio") or 0)
    prob = float(r.get("probability") or 0)
    
    ev = ""
    if stake > 0 and val_ratio > 0:
        ev = f"{(stake * (val_ratio - 1)):.2f}"
    
    prob_pct = f"{(prob * 100):.1f}%" if prob > 0 else ""
    
    val_pct = ""
    if val_ratio > 0:
        vp = (val_ratio - 1) * 100
        val_pct = f"{'+' if vp>0 else ''}{vp:.1f}%"

    return {
        "ID": r.get("bet_id", ""),
        "Date": r.get("date", ""),
        "Bookie": clean(r.get("book")),
        "Event": clean(r.get("event")),
        "Bet": clean(r.get("bet_text")),
        "Stake": r.get("stake", ""),
        "Odds": r.get("odds", ""),
        "Fair Odds": r.get("fair_odds", ""),
        "Prob %": prob_pct,
        "Value %": val_pct,
        "EV": ev,
        "Result": r.get("result", ""),
        "Profit": r.get("profit", "")
    }

def fetch_all_bets():
    """Scrapes all bets using the nonce and returns a list of processed rows."""
    # 1. Get a fresh session & nonce
    config = get_nonce_and_config()
    nonce = config.get("nonce")
    
    # 2. Start Scraping
    page = 1
    page_size = 2000
    all_rows = []
    
    # Use a session to keep cookies (even if user is guest, PHP session cookies might be required)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-WP-Nonce": nonce
    })
    
    while True:
        print(f"Fetching page {page}...")
        params = {
            "page": page,
            "pageSize": page_size,
            "mode": "flat",
            "_": int(time.time()*1000)
        }
        
        resp = session.get(API_URL, params=params)
        
        if resp.status_code == 403:
            print("Error 403: Security check failed.")
            break
            
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("rows", [])
        
        if not rows:
            break
            
        print(f"  + Got {len(rows)} bets.")
        all_rows.extend(rows)
        
        if len(rows) < page_size:
            break
        
        page += 1
        time.sleep(1)
        
    return all_rows

def main():
    try:
        raw_rows = fetch_all_bets()
        print(f"Successfully fetched {len(raw_rows)} bets.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
