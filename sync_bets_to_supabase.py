import os
import json
from dotenv import load_dotenv
from supabase import create_client, Client
from auto_extract import fetch_all_bets

# Load environment variables
load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

supabase: Client = create_client(url, key)

TABLE_NAME = os.environ.get("SUPABASE_TABLE_NAME", "value_betting_edge_viper_entries")

def get_max_existing_id():
    """Fetches the maximum ID currently in the Supabase table."""
    try:
        # Sort by id descending and limit to 1 to get the max
        response = supabase.table(TABLE_NAME).select("id").order("id", desc=True).limit(1).execute()
        data = response.data
        if data and len(data) > 0:
            return int(data[0]['id'])
        return 0
    except Exception as e:
        print(f"Error fetching max ID: {e}")
        return 0

def clean_float(val):
    """Converts value to float, handles None/empty strings."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        return None

def sync_bets():
    print("Fetching bets from source...")
    # Fetch all bets/raw rows
    raw_bets = fetch_all_bets()
    
    if not raw_bets:
        print("No bets found from source.")
        return

    print(f"Fetched {len(raw_bets)} total bets.")

    # Get the latest ID from Supabase to filter new ones
    max_id = get_max_existing_id()
    print(f"Max existing ID in DB: {max_id}")

    new_records = []
    
    for bet in raw_bets:
        try:
            bet_id = int(bet.get("bet_id", 0))
        except (ValueError, TypeError):
            continue

        # Only process if ID is newer than what we have
        if bet_id > max_id:
            # Map fields logic
            # Database columns:
            # id, date, bookie, selection, stake, odds, closing_odds, result, event
            
            record = {
                "id": bet_id,
                "date": bet.get("date"), # raw date format '2023-11-28' usually fine for timestamp
                "bookie": bet.get("book"),
                "selection": bet.get("bet_text"),
                "event": bet.get("event"),
                "stake": clean_float(bet.get("stake")),
                "odds": clean_float(bet.get("odds")),
                "closing_odds": clean_float(bet.get("fair_odds")),
                "result": bet.get("result")
            }
            new_records.append(record)
    
    if not new_records:
        print("No new data to post.")
        return

    print(f"Found {len(new_records)} new records to insert.")
    
    # Inserting in batches to be efficient and avoid payload limits
    BATCH_SIZE = 1000
    for i in range(0, len(new_records), BATCH_SIZE):
        batch = new_records[i : i + BATCH_SIZE]
        print(f"Inserting batch {i // BATCH_SIZE + 1} ({len(batch)} records)...")
        try:
            response = supabase.table(TABLE_NAME).insert(batch).execute()
            # In older supabase-py versions .execute() might not return much, but if it doesn't throw, it's usually good.
        except Exception as e:
            print(f"Error inserting batch: {e}")

    print("Sync complete.")

if __name__ == "__main__":
    sync_bets()
