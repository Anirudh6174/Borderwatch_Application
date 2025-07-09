import os
import json
import requests
from datetime import datetime, timedelta
from shapely.geometry import shape
import time # Import time for delays

# --- Configuration ---
# Load credentials from environment variables for better security
COPERNICUS_USERNAME = os.getenv("COPERNICUS_USERNAME")
COPERNICUS_PASSWORD = os.getenv("COPERNICUS_PASSWORD")

if not COPERNICUS_USERNAME or not COPERNICUS_PASSWORD:
    print("ERROR: Copernicus username or password not set as environment variables.")
    print("Please set COPERNICUS_USERNAME and COPERNICUS_PASSWORD before running.")
    print("Register at: https://dataspace.copernicus.eu/")
    exit()

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config')
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data')
ROI_FILE = os.path.join(CONFIG_DIR, 'rois.json')

# Copernicus Data Space Ecosystem API endpoints
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def get_access_token():
    """Get access token for Copernicus Data Space Ecosystem API"""
    try:
        data = {
            "grant_type": "password",
            "username": COPERNICUS_USERNAME,
            "password": COPERNICUS_PASSWORD,
            "client_id": "cdse-public"
        }
        
        response = requests.post(TOKEN_URL, data=data, timeout=30)
        response.raise_for_status()
        
        token_data = response.json()
        return token_data.get("access_token")
    
    except requests.exceptions.RequestException as e:
        print(f"Error getting access token: {e}")
        return None

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

def geometry_to_wkt(geometry):
    """Convert GeoJSON geometry to WKT format for API queries"""
    try:
        geom = shape(geometry)
        return geom.wkt
    except Exception as e:
        print(f"Error converting geometry to WKT: {e}")
        return None

def search_products(access_token, roi_feature, start_date, end_date, collection_name, cloud_cover_max=20, product_type_filter=None):
    """Search for satellite products using the Copernicus Data Space Ecosystem API"""
    
    roi_name = roi_feature['properties'].get('name', 'Unnamed_ROI')
    geometry_wkt = geometry_to_wkt(roi_feature['geometry'])
    
    if not geometry_wkt:
        print(f"Error: Could not convert geometry to WKT for ROI: {roi_name}")
        return []
    
    print(f"\nSearching for {collection_name} data for ROI: {roi_name}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        # Build the OData filter
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        filter_query = f"Collection/Name eq '{collection_name}' and ContentDate/Start ge {start_date_str} and ContentDate/Start le {end_date_str} and OData.CSC.Intersects(area=geography'SRID=4326;{geometry_wkt}')"
        
        # Add cloud cover filter for optical data
        if collection_name == 'SENTINEL-2':
            filter_query += f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_cover_max})"
            # Add product type filter for Sentinel-2
            if product_type_filter:
                filter_query += f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type_filter}')"
        
        params = {
            '$filter': filter_query,
            '$orderby': 'ContentDate/Start desc',
            '$top': 100,  # Limit results
            '$format': 'json'
        }
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json'
        }
        
        response = requests.get(CATALOG_URL, params=params, headers=headers, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        products = data.get('value', [])
        
        print(f"Found {len(products)} {collection_name} products for '{roi_name}'")
        
        return products
        
    except requests.exceptions.RequestException as e:
        print(f"Error searching for {collection_name} products: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error during search: {e}")
        return []

def download_product(access_token, product, download_path):
    """Download a single product"""
    try:
        product_id = product['Id']
        product_name = product['Name']
        
        # Check if product already exists
        product_filename = f"{product_name}.zip"
        full_path = os.path.join(download_path, product_filename)
        
        if os.path.exists(full_path):
            print(f"Skipping {product_name} (already downloaded)")
            return True
        
        print(f"Downloading {product_name}...")
        
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        
        download_url = f"{DOWNLOAD_URL}({product_id})/$value"
        
        response = requests.get(download_url, headers=headers, stream=True, timeout=300)
        response.raise_for_status()
        
        # Download with progress
        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"Successfully downloaded {product_name}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {product['Name']}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error downloading {product['Name']}: {e}")
        return False

def download_satellite_data(access_token, roi_feature, start_date, end_date, collection_name, max_downloads=5, product_type_filter=None):
    """Query and download satellite data for a given ROI"""
    
    roi_name = roi_feature['properties'].get('name', 'Unnamed_ROI')
    
    # Search for products
    products = search_products(access_token, roi_feature, start_date, end_date, collection_name, product_type_filter=product_type_filter)
    
    if not products:
        print(f"No {collection_name} products found for '{roi_name}'")
        return
    
    # Create download directory
    download_path = os.path.join(DATA_DIR, roi_name, collection_name)
    os.makedirs(download_path, exist_ok=True)
    
    # Download products (limit to avoid excessive downloads)
    downloaded_count = 0
    for product in products[:max_downloads]:
        if download_product(access_token, product, download_path):
            downloaded_count += 1
        
        # Add a small delay between downloads to be polite to the API
        time.sleep(1)
    
    print(f"Downloaded {downloaded_count} products for {roi_name} ({collection_name})")

if __name__ == "__main__":
    # Get access token
    print("Authenticating with Copernicus Data Space Ecosystem...")
    access_token = get_access_token()
    
    if not access_token:
        print("Failed to get access token. Please check your credentials.")
        exit()
    
    print("Authentication successful!")
    
    # Load ROIs
    rois = load_rois(ROI_FILE)
    if not rois:
        print("No ROIs loaded. Please check 'config/rois.json'. Exiting.")
        exit()
    
    # Define date range for data acquisition
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    for roi in rois:
        # Download Sentinel-1 (SAR) data
        download_satellite_data(access_token, roi, start_date, end_date, 'SENTINEL-1', max_downloads=3)
        
        # Download Sentinel-2 (Optical) data (try Level-1C first for broader access)
        download_satellite_data(access_token, roi, start_date, end_date, 'SENTINEL-2', max_downloads=3, product_type_filter='S2MSI1C')
    
    print("\nData download process complete.")