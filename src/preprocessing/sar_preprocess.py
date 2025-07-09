# src/preprocessing/sar_preprocess.py
import os
import rasterio
import rioxarray as rxr
import xarray as xr
from datetime import datetime
from shapely.geometry import shape
import json
from skimage.restoration import denoise_nl_means # A simple speckle filter
import numpy as np
import glob
import time # Ensure time is imported

# --- Configuration ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/processed')
ROI_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config/rois.json')

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Placeholder for DEM path (replace with actual path or download logic)
DEM_PATH = "path/to/your/dem.tif" # E.g., a SRTM DEM tile covering your ROI

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

# --- MODIFIED FUNCTION ---
def find_sar_tiff_files(roi_name):
    """
    Finds Sentinel-1 .tiff files within .SAFE directories for a given ROI.
    This version is tailored for COG variants of S1 .SAFE products.
    """
    sar_roi_path = os.path.join(DATA_DIR, roi_name, 'SENTINEL-1')
    if not os.path.exists(sar_roi_path):
        print(f"SAR data directory not found for ROI: {roi_name}")
        return []
    
    tiff_files = []
    # Find all .SAFE directories (e.g., S1A_IW_GRDH_1SDV_2025... .SAFE)
    # The 'COG' suffix is often part of the SAFE directory name itself for COG products
    safe_dirs = glob.glob(os.path.join(sar_roi_path, 'S1*_IW_GRDH_*_COG.SAFE'))
    
    # If no COG.SAFE found, try without COG suffix in case of other S1 GRD SAFE products
    if not safe_dirs:
        safe_dirs = glob.glob(os.path.join(sar_roi_path, 'S1*_IW_GRDH_*.SAFE'))

    if not safe_dirs:
        print(f"No Sentinel-1 .SAFE directories found in {sar_roi_path}. Ensure products are unzipped here.")
        return []

    for safe_dir in safe_dirs:
        # Based on your provided path, TIFFs are directly under 'measurement/'
        # The filename also contains '-cog.tiff'
        pattern = os.path.join(safe_dir, 'measurement', 's1*-iw-grd-*.tiff')
        current_tiff_files = glob.glob(pattern)
        
        if current_tiff_files:
            tiff_files.extend(current_tiff_files)
        else:
            print(f"Warning: No TIFF files found within expected path inside {os.path.basename(safe_dir)}. Checked pattern: {pattern}")
            print("Please manually verify the internal structure of this .SAFE directory if issues persist.")

    print(f"Found {len(tiff_files)} TIFF files for SAR preprocessing in {roi_name}.")
    return tiff_files
# --- END MODIFIED FUNCTION ---


def process_sar_image(sar_image_path, roi_geometry, dem_path=None):
    """
    Performs basic preprocessing steps on a single Sentinel-1 GRD SAR image.
    This is a simplified pipeline. For full robustness, consider using SNAP via snappy, or more advanced libraries.
    """
    print(f"Processing SAR image: {os.path.basename(sar_image_path)}")
    try:
        # 1. Read SAR Data
        sar_data = rxr.open_rasterio(sar_image_path, masked=True).squeeze()

        if sar_data.ndim > 2:
            print(f"Multiple bands found ({sar_data.ndim}). Selecting first band for processing.")
            sar_data = sar_data.isel(band=0) 

        sar_data = sar_data.astype(np.float32)

        # 2. Radiometric Calibration (simplified) - typically already done for GRD
        
        # 3. Speckle Filtering
        print("Applying speckle filter...")
        min_val, max_val = sar_data.min().item(), sar_data.max().item()
        if max_val - min_val > 0:
            normalized_sar = (sar_data - min_val) / (max_val - min_val)
            filtered_sar = denoise_nl_means(normalized_sar.values, h=0.1, fast_mode=True, patch_size=5, patch_distance=6)
            filtered_sar = filtered_sar * (max_val - min_val) + min_val
        else:
            filtered_sar = sar_data.values
        sar_data.values = filtered_sar
        print("Speckle filtering complete.")

        # 4. Terrain Correction / Orthorectification (Conceptual for now)
        target_crs = "EPSG:32643" 
        try:
            if str(sar_data.rio.crs) != target_crs:
                print(f"Reprojecting from {sar_data.rio.crs} to {target_crs}...")
                sar_data_reprojected = sar_data.rio.reproject(target_crs)
                print(f"Reprojected to {target_crs}.")
            else:
                print(f"SAR data already in {target_crs}.")
                sar_data_reprojected = sar_data
        except Exception as e:
            print(f"Could not reproject: {e}. Proceeding without reprojection to {target_crs}.")
            sar_data_reprojected = sar_data

        # --- DEBUGGING LINES START ---
        print(f"SAR Data Bounds (after reprojection to {sar_data_reprojected.rio.crs}): {sar_data_reprojected.rio.bounds()}")
        
        # Correct way to get ROI bounds using shapely object
        roi_shapely = shape(roi_geometry) # Create the shapely object here
        minx_roi_orig, miny_roi_orig, maxx_roi_orig, maxy_roi_orig = roi_shapely.bounds
        print(f"ROI Geometry Bounds (original CRS, likely EPSG:4326): ({minx_roi_orig}, {miny_roi_orig}, {maxx_roi_orig}, {maxy_roi_orig})")

        if sar_data_reprojected.rio.crs and str(sar_data_reprojected.rio.crs) != "EPSG:4326": # Assuming ROI is 4326
            try:
                transformer = Transformer.from_crs("EPSG:4326", sar_data_reprojected.rio.crs, always_xy=True)
                # Transform the corner points of the ROI's original bounding box for comparison
                corners = [(minx_roi_orig, miny_roi_orig), (maxx_roi_orig, miny_roi_orig),
                           (minx_roi_orig, maxy_roi_orig), (maxx_roi_orig, maxy_roi_orig)]
                transformed_corners = [transformer.transform(x, y) for x, y in corners]
                
                reprojected_roi_minx = min(p[0] for p in transformed_corners)
                reprojected_roi_miny = min(p[1] for p in transformed_corners)
                reprojected_roi_maxx = max(p[0] for p in transformed_corners)
                reprojected_roi_maxy = max(p[1] for p in transformed_corners)

                print(f"ROI Geometry Bounds (reprojected to {sar_data_reprojected.rio.crs}): ({reprojected_roi_minx}, {reprojected_roi_miny}, {reprojected_roi_maxx}, {reprojected_roi_maxy})")
            except Exception as e:
                print(f"Warning: Could not reproject ROI bounds for comparison: {e}")

        # TEMPORARY DEBUGGING: Save the reprojected SAR data BEFORE clipping
        debug_output_dir = os.path.join(PROCESSED_DATA_DIR, roi_name, 'SENTINEL-1_DEBUG')
        os.makedirs(debug_output_dir, exist_ok=True)

        temp_debug_path = os.path.join(debug_output_dir, f"DEBUG_{os.path.basename(sar_image_path).replace('.tiff', '_reprojected.tif')}")
        print(f"DEBUG: Saving reprojected SAR data to {temp_debug_path} for inspection.")
        if sar_data_reprojected.rio.crs is None:
            sar_data_reprojected = sar_data_reprojected.rio.write_crs(target_crs) 
        sar_data_reprojected.rio.to_raster(temp_debug_path)
        print("DEBUG: Reprojected SAR data saved. Please inspect this file in QGIS along with your ROI GeoJSON.")
        
        # --- DEBUGGING LINES END ---


        # 5. Convert to Decibels (dB)
        print("Converting to Decibels (dB)...")
        sar_data_db = 10 * np.log10(np.maximum(sar_data_reprojected, 1e-5)) 
        print("Converted to dB.")

        # 6. Clipping to ROI
        print("Clipping to ROI...")
        # The roi_shapely object is already created above in the DEBUG block.

        # *** KEEP THE FOLLOWING TWO LINES COMMENTED OUT FOR NOW TO ALLOW INSPECTION ***
        # clipped_sar = sar_data_db.rio.clip([roi_shapely], drop=True, crs=sar_data_db.rio.crs)
        # print("Clipped to ROI.")
        # *** END COMMENT OUT ***

        # *** FOR DEBUGGING, RETURN THE UNCLIPPED, PROCESSED DATA ***
        return sar_data_db # Return the reprojected, unclipped, dB-converted data for inspection

    except Exception as e:
        print(f"Error processing SAR image {os.path.basename(sar_image_path)}: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    rois = load_rois(ROI_FILE)
    if not rois:
        print("No ROIs loaded. Exiting SAR preprocessing.")
        exit()

    for roi in rois:
        roi_name = roi['properties'].get('name', 'Unnamed_ROI')
        # --- MODIFIED CALL ---
        sar_files = find_sar_tiff_files(roi_name) # Call the new function
        # --- END MODIFIED CALL ---
        
        if not sar_files:
            print(f"No SAR TIFF files found to process for ROI: {roi_name}. Skipping preprocessing for this ROI.")
            continue

        processed_roi_dir = os.path.join(PROCESSED_DATA_DIR, roi_name, 'SENTINEL-1')
        os.makedirs(processed_roi_dir, exist_ok=True)

        for sar_file_path in sar_files:
            # Construct output filename
            base_filename = os.path.splitext(os.path.basename(sar_file_path))[0]
            output_filename = f"{base_filename}_preprocessed.tif"
            output_path = os.path.join(processed_roi_dir, output_filename)

            if os.path.exists(output_path):
                print(f"Skipping {output_filename} (already processed).")
                continue

            processed_sar = process_sar_image(sar_file_path, roi['geometry'], dem_path=DEM_PATH)
            
            if processed_sar is not None:
                print(f"Saving processed SAR image to: {output_path}")
                # Save the processed data as a GeoTIFF
                # Ensure the CRS is set on the xarray object before saving
                if processed_sar.rio.crs is None:
                    print("Warning: CRS not set on processed_sar. Setting to EPSG:4326 for saving.")
                    processed_sar = processed_sar.rio.write_crs("EPSG:4326")
                processed_sar.rio.to_raster(output_path)
                print("Saved successfully.")
            else:
                print(f"Failed to process {os.path.basename(sar_file_path)}.")
    
    print("\nSAR Preprocessing complete for all ROIs.")