# src/data_ingestion/download_data.py
from sentinelsat import SentinelAPI
import os
import json
from datetime import datetime, timedelta

# --- Configuration ---
# Load credentials from environment variables for better security
# On Linux/macOS: export COPERNICUS_USERNAME="your_user"
# On Windows (cmd): set COPERNICUS_USERNAME="your_user"
COPERNICUS_USERNAME = os.getenv("COPERNICUS_USERNAME")
COPERNICUS_PASSWORD = os.getenv("COPERNICUS_PASSWORD")

if not COPERNICUS_USERNAME or not COPERNICUS_PASSWORD:
    print("ERROR: Copernicus username or password not set as environment variables.")
    print("Please set COPERNICUS_USERNAME and COPERNICUS_PASSWORD before running.")
    exit()

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config')
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data')
ROI_FILE = os.path.join(CONFIG_DIR, 'rois.json')

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def load_rois(roi_file_path):
    """Loads ROIs from a GeoJSON file."""
    try:
        with open(roi_file_path, 'r') as f:
            geojson_data = json.load(f)
        return geojson_data['features']
    except FileNotFoundError:
        print(f"Error: ROI file not found at {roi_file_path}")
        return []
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON from {roi_file_path}")
        return []

def download_satellite_data(api, roi_feature, start_date, end_date, platform_name, product_type, cloud_cover_percentage=(0, 20)):
    """
    Queries and downloads satellite data for a given ROI.
    """
    roi_name = roi_feature['properties'].get('name', 'Unnamed_ROI')
    footprint = json.dumps(roi_feature['geometry']) # GeoJSON as string

    print(f"\nSearching for {platform_name} data for ROI: {roi_name} ({start_date} to {end_date})...")

    try:
        products = api.query(footprint,
                             date=(start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),
                             platformname=platform_name,
                             producttype=product_type,
                             cloudcoverpercentage=cloud_cover_percentage)

        print(f"Found {len(products)} {platform_name} products for '{roi_name}'.")

        if products:
            # Create a specific directory for the ROI and platform
            download_path = os.path.join(DATA_DIR, roi_name, platform_name)
            os.makedirs(download_path, exist_ok=True)

            downloaded_count = 0
            for product_id, product_info in products.items():
                # Check if product already exists to avoid re-downloading
                product_filename = product_info['title'] + '.zip'
                if os.path.exists(os.path.join(download_path, product_filename)):
                    print(f"Skipping {product_info['title']} (already downloaded).")
                    continue

                print(f"Downloading {product_info['title']}...")
                try:
                    api.download(product_id, directory_path=download_path)
                    print(f"Downloaded {product_info['title']}")
                    downloaded_count += 1
                except Exception as e:
                    print(f"Failed to download {product_info['title']}: {e}")
            print(f"Total downloaded for {roi_name} ({platform_name}): {downloaded_count} products.")
        else:
            print(f"No {platform_name} products found for '{roi_name}' within the specified criteria.")

    except Exception as e:
        print(f"An error occurred during query or download for {platform_name} data: {e}")

if __name__ == "__main__":
    api = SentinelAPI(COPERNICUS_USERNAME, COPERNICUS_PASSWORD, 'https://creodias.sentinel-hub.com/adfs/oauth2/token')

    rois = load_rois(ROI_FILE)
    if not rois:
        print("No ROIs loaded. Please check 'config/rois.json'. Exiting.")
        exit()

    # Define date range for data acquisition
    # For initial testing, let's grab data from the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    for roi in rois:
        # Download Sentinel-1 (SAR) data
        download_satellite_data(api, roi, start_date, end_date,
                                platform_name='Sentinel-1',
                                product_type='GRD') # Ground Range Detected, Dual polarization (GRD)

        # Download Sentinel-2 (Optical) data (Level-2A is atmospherically corrected)
        download_satellite_data(api, roi, start_date, end_date,
                                platform_name='Sentinel-2',
                                product_type='S2MSI2A',
                                cloud_cover_percentage=(0, 10)) # Max 10% cloud cover for optical

    print("\nData download process complete.")