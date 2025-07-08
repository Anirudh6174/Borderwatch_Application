import requests
import json
from datetime import datetime, timedelta

def test_copernicus_connection():
    """Test connection to Copernicus Data Space Ecosystem API"""
    
    print("Attempting to connect to Copernicus Data Space Ecosystem...")
    
    try:
        # Correct Copernicus Data Space Ecosystem API base URL
        base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1"
        
        # Test connection with a simple query
        test_url = f"{base_url}/Products"
        
        # Add headers to identify as API request
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Python-requests/2.31.0'
        }
        
        # Make a simple test request with pagination limit
        params = {
            '$top': 1,  # Limit to 1 result for testing
            '$format': 'json'
        }
        
        response = requests.get(test_url, headers=headers, params=params, timeout=30)
        
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            try:
                # Try to parse as JSON
                data = response.json()
                print("Successfully connected to Copernicus API!")
                print(f"Response type: {type(data)}")
                
                if 'value' in data:
                    print(f"Found {len(data['value'])} products in test query")
                    if data['value']:
                        print("Sample product keys:", list(data['value'][0].keys()))
                else:
                    print("Unexpected response structure:", list(data.keys()) if isinstance(data, dict) else "Non-dict response")
                    
            except json.JSONDecodeError:
                print("Failed to parse JSON response")
                print("Response content (first 500 chars):", response.text[:500])
                
        else:
            print(f"HTTP Error: {response.status_code}")
            print("Response content:", response.text[:500])
            
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def search_sentinel_products():
    """Search for Sentinel-2 products with proper API endpoint"""
    
    print("\n" + "="*50)
    print("Searching for Sentinel-2 products...")
    
    try:
        # Correct API endpoint for Copernicus Data Space Ecosystem
        base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1"
        
        # Calculate date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Format dates for OData query
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Build OData query for Sentinel-2 products
        query_params = {
            '$filter': f"Collection/Name eq 'SENTINEL-2' and ContentDate/Start ge {start_date_str} and ContentDate/Start le {end_date_str}",
            '$top': 5,
            '$orderby': 'ContentDate/Start desc',
            '$format': 'json'
        }
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Python-requests/2.31.0'
        }
        
        response = requests.get(f"{base_url}/Products", params=query_params, headers=headers, timeout=30)
        
        print(f"Query URL: {response.url}")
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                products = data.get('value', [])
                
                print(f"Found {len(products)} Sentinel-2 products")
                
                if products:
                    print("\nSample products:")
                    for i, product in enumerate(products[:3]):  # Show first 3
                        print(f"  {i+1}. {product.get('Name', 'N/A')}")
                        print(f"     Date: {product.get('ContentDate', {}).get('Start', 'N/A')}")
                        print(f"     Size: {product.get('ContentLength', 'N/A')} bytes")
                        print()
                else:
                    print("No products found for the specified criteria")
                    
            except json.JSONDecodeError:
                print("Failed to parse JSON response")
                print("Response content:", response.text[:500])
                
        else:
            print(f"HTTP Error: {response.status_code}")
            print("Response content:", response.text[:500])
            
    except Exception as e:
        print(f"Error searching products: {e}")

if __name__ == "__main__":
    test_copernicus_connection()
    search_sentinel_products()