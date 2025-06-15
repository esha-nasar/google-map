import pandas as pd
import requests
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- Configuration ---
GOOGLE_MAPS_API_KEY = '' 
FIXED_POINT = "Times Square, New York, NY"        
CSV_FILE = "addresses.csv"
GOOGLE_SHEET_NAME = "Address Distance Report"
CREDENTIALS_FILE = "credentials.json"

# --- Step 1: Load addresses from CSV ---
df = pd.read_csv(CSV_FILE)
if 'address' not in df.columns:
    raise ValueError("CSV must have a column named 'address'")

# --- Step 2: Get latitude & longitude using Geocoding API ---
def get_coordinates(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {'address': address, 'key': GOOGLE_MAPS_API_KEY}
    response = requests.get(url, params=params).json()
    if response['status'] == 'OK':
        location = response['results'][0]['geometry']['location']
        return location['lat'], location['lng']
    return None, None

# --- Step 3: Get distance from fixed point using Distance Matrix API ---
def get_distance(origin, destination):
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        'origins': origin,
        'destinations': destination,
        'key': GOOGLE_MAPS_API_KEY,
        'units': 'metric'
    }
    response = requests.get(url, params=params).json()
    try:
        distance = response['rows'][0]['elements'][0]['distance']['text']
        return distance
    except:
        return "N/A"

# --- Step 4: Process addresses ---
latitudes = []
longitudes = []
distances = []

for address in df['address']:
    print(f"Processing: {address}")
    lat, lng = get_coordinates(address)
    latitudes.append(lat)
    longitudes.append(lng)
    dist = get_distance(FIXED_POINT, address)
    distances.append(dist)
    time.sleep(1)  # Respect API rate limit

df['Latitude'] = latitudes
df['Longitude'] = longitudes
df['Distance_from_' + FIXED_POINT] = distances

# --- Step 5: Upload to Google Sheets ---
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
client = gspread.authorize(creds)

sheet = client.create(GOOGLE_SHEET_NAME)
sheet.share('esha168nasar@gmail.com', perm_type='user', role='writer')
print("📄 Sheet created at:", sheet.url)
worksheet = sheet.get_worksheet(0)
df = df.fillna('')
# Add data to the sheet
worksheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ Data successfully uploaded to Google Sheet!")
