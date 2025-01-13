import json

# Define the script to validate coordinates within NYC boundaries
def validate_coordinates():
    # NYC bounding box (approximate)
    NYC_BOUNDS = {
        "min_lat": 40.4774,
        "max_lat": 40.9176,
        "min_lon": -74.2591,
        "max_lon": -73.7004
    }

    try:
        # Load arrest data with GEOIDs
        with open('/tmp/nyc_arrest_data.json', 'r') as f:
            arrest_data = json.load(f)

        validated_data = []

        for record in arrest_data:
            lat = record.get("latitude")
            lon = record.get("longitude")

            if lat and lon:
                lat, lon = float(lat), float(lon)
                if (NYC_BOUNDS["min_lat"] <= lat <= NYC_BOUNDS["max_lat"] and
                        NYC_BOUNDS["min_lon"] <= lon <= NYC_BOUNDS["max_lon"]):
                    validated_data.append(record)

        # Save validated data
        with open('/tmp/nyc_arrest_coordinates_validated.json', 'w') as f:
            json.dump(validated_data, f)
        print("Arrest data validated for NYC boundaries and saved.")

    except Exception as e:
        print(f"Error during validation: {e}")