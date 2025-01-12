import json
import requests

# Define the script to derive GEOID for each arrest record
def derive_geoid():
    # Load arrest data
    try:
        with open('/tmp/nyc_arrest_coordinates_validated.json', 'r') as f:
            arrest_data = json.load(f)
    except Exception as e:
        print(f"Error loading arrest data: {e}")
        return

    # Census Geocoder API endpoint
    GEOCODER_API = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

    updated_data = []

    for record in arrest_data:
        lat = record.get("latitude")
        lon = record.get("longitude")

        if not lat or not lon:
            record["geoid"] = None
            updated_data.append(record)
            continue

        params = {
            "x": lon,
            "y": lat,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json"
        }

        try:
            response = requests.get(GEOCODER_API, params=params)
            response.raise_for_status()

            geographies = response.json()
            geoid = geographies.get("result", {}).get("geographies", {}).get("Census Tracts", [{}])[0].get("GEOID")
            record["geoid"] = geoid
        except Exception as e:
            print(f"Error retrieving GEOID for record with lat={lat}, lon={lon}: {e}")
            record["geoid"] = None

        updated_data.append(record)

    # Save updated data with GEOID
    print(json.dumps(updated_data, indent=4))
    try:
        with open('/tmp/nyc_arrest_data_with_geoid.json', 'w') as f:
            json.dump(updated_data, f)
        print("Arrest data updated with GEOIDs and saved.")
    except Exception as e:
        print(f"Error saving updated arrest data: {e}")