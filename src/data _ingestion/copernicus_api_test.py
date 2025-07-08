# src/data_ingestion/copernicus_api_test.py
from sentinelsat import SentinelAPI
import os


# For this initial test, we'll put them directly, but we'll refactor later.
COPERNICUS_USERNAME = "anirudh.rich@gmail.com"
COPERNICUS_PASSWORD = "3#B_YR@2h.ivTcL"

def test_copernicus_connection():
    print("Attempting to connect to Copernicus Data Space Ecosystem...")
    try:
        api = SentinelAPI(COPERNICUS_USERNAME, COPERNICUS_PASSWORD, 'https://creodias.sentinel-hub.com/adfs/oauth2/token')
        print("Successfully connected to Copernicus API!")
        # A simple search to confirm connection
        # Let's search for some Sentinel-2 data over a small area near Ghaziabad
        # Bounding box for a small area in Uttar Pradesh (approx. Ghaziabad)
        # You can get bounding boxes from tools like geojson.io
        footprint = 'POLYGON((77.30 28.60, 77.40 28.60, 77.40 28.70, 77.30 28.70, 77.30 28.60))'

        # Search for Sentinel-2 L2A (processed data) from last week
        products = api.query(footprint,
                             date = ('NOW-7DAYS', 'NOW'),
                             platformname = 'Sentinel-2',
                             producttype = 'S2MSI2A', # Sentinel-2 MultiSpectral Instrument Level-2A (Bottom of Atmosphere corrected)
                             cloudcoverpercentage = (0, 10)) # Max 10% cloud cover

        print(f"Found {len(products)} Sentinel-2 products in the last 7 days with <10% cloud cover.")
        if products:
            print("Example product ID:", list(products.keys())[0])
        else:
            print("No products found for the specified criteria. Try adjusting date or cloud cover.")

    except Exception as e:
        print(f"Failed to connect or query Copernicus API: {e}")

if __name__ == "__main__":
    test_copernicus_connection()