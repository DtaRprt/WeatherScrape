import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- CONFIGURATION ---
# Save directly to the current folder (repository root)
CSV_FILE_PATH = "BTAC_History.csv"
BASE_URL = "https://view.btjhwxavyproject.com/php-api/am_wx.php"

# --- HELPER: LOGGING ---
def write_log(message):
    try:
        log_file = os.path.join(os.path.dirname(CSV_FILE_PATH), "scrape_log.txt")
        with open(log_file, "a") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        print(f"Logging failed: {e}")

# --- HELPER: DATE CONVERSION ---
def get_prophix_date(date_obj):
    """
    Converts a standard date to Prophix format (YYYYDddd).
    Rule: The year rolls over on May 1st.
    Example: May 1, 2026 -> 2026D001
             Dec 6, 2025 -> 2025D220 (approx)
    """
    if date_obj.month >= 5:
        # We are in the current year's cycle (May - Dec)
        start_year = date_obj.year
    else:
        # We are in the previous year's cycle (Jan - Apr)
        start_year = date_obj.year - 1
        
    fiscal_start = datetime(start_year, 5, 1)
    day_num = (date_obj - fiscal_start).days + 1
    
    return f"{start_year}D{day_num:03d}"

# --- MAIN FUNCTIONS ---
def get_btac_data():
    # We run at 5:30 AM to get the PRIOR day's full 24h cycle
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    
    timestamp = int(time.time() * 1000)
    params = {
        "action": "getday",
        "day": date_str,
        "_": timestamp
    }
    
    print(f"Fetching data for {date_str}...")
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://bridgertetonavalanchecenter.org/",
            "Origin": "https://bridgertetonavalanchecenter.org"
        }
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json(), yesterday
    except Exception as e:
        msg = f"Error fetching data: {e}"
        print(msg)
        write_log(msg)
        return None, None

def process_data(json_payload, date_obj):
    if not json_payload or 'data' not in json_payload:
        print("Invalid JSON structure (missing 'data' key).")
        return pd.DataFrame()

    rows_data = json_payload['data']
    
    # Calculate Prophix Date using Custom Logic
    prophix_date = get_prophix_date(date_obj)
    
    extracted_rows = []

    # --- RAYMER MERGE LOGIC ---
    raymer_temp_node = next((r for r in rows_data if "raymer 9,360" in str(r.get('display_name','')).lower()), {})
    raymer_wind_node = next((r for r in rows_data if "raymer wind" in str(r.get('display_name','')).lower()), {})
    
    raymer_merged = {
        'display_name': "Raymer",
        'maxtemp': raymer_temp_node.get('maxtemp'),
        'mintemp': raymer_temp_node.get('mintemp'),
        'avewindspd': raymer_wind_node.get('avewindspd'),
        'maxgust': raymer_wind_node.get('maxgust'),
        'ttlwindmiles': raymer_wind_node.get('ttlwindmiles'),
        'newsnow': raymer_temp_node.get('newsnow') or raymer_wind_node.get('newsnow'),
        'depth': raymer_temp_node.get('depth') or raymer_wind_node.get('depth'),
        'ttlsnowfall': raymer_temp_node.get('ttlsnowfall') or raymer_wind_node.get('ttlsnowfall'),
    }

    # --- DEFINE TARGETS ---
    target_defs = [
        ("Summit", ["summit"], False),
        ("RV_Bowl", ["rbowl", "rendezvous bowl"], False),
        ("Raymer", [], True), 
        ("MidMtn", ["mid mtn", "mid mountain"], False),
        ("Buff", ["buff"], False),
        ("Base", ["base"], False)
    ]

    print(f"Scanning {len(rows_data)} stations for Prophix Date: {prophix_date}")

    for target_name, keywords, is_merged in target_defs:
        found_station = None
        
        if is_merged and target_name == "Raymer":
            found_station = raymer_merged
        else:
            for row in rows_data:
                d_name = str(row.get('display_name', '')).lower()
                if any(k in d_name for k in keywords):
                    found_station = row
                    break
        
        if found_station:
            new_row = {
                'ProphixDate': prophix_date,
                'Date': date_obj.strftime('%Y-%m-%d'),
                'Location': target_name,
                
                # Corrected Keys
                'NewSno': found_station.get('newsnow', ''),
                'SnoDepth': found_station.get('depth', ''),
                'SnoFall Tot': found_station.get('ttlsnowfall', ''),
                'Max Temp': found_station.get('maxtemp', ''),
                'Min Temp': found_station.get('mintemp', ''),
                'AvgWind': found_station.get('avewindspd', ''),
                'MaxGust': found_station.get('maxgust', ''),
                'TotalWind': found_station.get('ttlwindmiles', ''),
            }
            extracted_rows.append(new_row)
        else:
            print(f"  - Warning: {target_name} not found.")

    return pd.DataFrame(extracted_rows)

def main():
    # --- FIX START ---
    # Only try to create folders if the path actually has folders
    directory = os.path.dirname(CSV_FILE_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    # --- FIX END ---

    write_log("Starting Daily Scrape...")

    data, date_obj = get_btac_data()
    
    if data:
        df_new = process_data(data, date_obj)
        
        if not df_new.empty:
            cols = ['ProphixDate','Date', 'Location', 'NewSno', 'SnoDepth', 'SnoFall Tot', 
                    'Max Temp', 'Min Temp', 'AvgWind', 'MaxGust', 'TotalWind']
            
            for c in cols:
                if c not in df_new.columns: df_new[c] = ''
            df_new = df_new[cols]

            file_exists = os.path.isfile(CSV_FILE_PATH)
            
            try:
                df_new.to_csv(CSV_FILE_PATH, mode='a', header=not file_exists, index=False)
                msg = f"SUCCESS: Appended {len(df_new)} rows for {date_obj.strftime('%Y-%m-%d')}."
                print(msg)
                write_log(msg)
            except PermissionError:
                msg = f"ERROR: Could not write to {CSV_FILE_PATH}. Is the Excel file open?"
                print(msg)
                write_log(msg)
        else:
            msg = "No data rows matched the target stations."
            print(msg)
            write_log(msg)
    else:
        write_log("API request failed (Check internet or URL).")

if __name__ == "__main__":

    main()


