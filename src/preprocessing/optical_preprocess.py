import os
import rasterio
import rioxarray as rxr
import xarray as xr
from datetime import datetime
from shapely.geometry import shape
import json
import numpy as np
import glob
import re # For regular expressions to parse band names

# --- Configuration ---
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data/processed')
ROI_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config/rois.json')

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

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

def find_s2_jp2_files(roi_name, product_level='L1C'):
    """
    Finds Sentinel-2 .jp2 image files within .SAFE directories for a given ROI.
    Handles both L1C and L2A structures.
    """
    s2_roi_path = os.path.join(DATA_DIR, roi_name, 'SENTINEL-2')
    if not os.path.exists(s2_roi_path):
        print(f"Sentinel-2 data directory not found for ROI: {roi_name}")
        return []

    jp2_files = []
    # Find all .SAFE directories (e.g., S2A_MSIL1C_... .SAFE or S2B_MSIL2A_... .SAFE)
    # Using a more general glob for SAFE directories as product level might vary in download
    safe_dirs = glob.glob(os.path.join(s2_roi_path, 'S2*_MSIL*C_*.SAFE'))

    if not safe_dirs:
        print(f"No Sentinel-2 .SAFE directories found in {s2_roi_path}. Ensure products are unzipped here.")
        return []

    for safe_dir in safe_dirs:
        # Construct pattern based on typical S2 SAFE structure
        # Example: SAFE/GRANULE/L1C_Txxxx_Axxxx/IMG_DATA/Txxxx_B0X.jp2
        # Or for L2A: SAFE/GRANULE/L2A_Txxxx_Axxxx/IMG_DATA/Txxxx_B0X.jp2 (and also SCL)

        # Common bands for analysis (10m & 20m resolutions)
        # B02 (Blue), B03 (Green), B04 (Red), B08 (NIR) are 10m
        # B05, B06, B07, B8A (NIR Narrow), B11 (SWIR1), B12 (SWIR2) are 20m
        # B01, B09, B10 (Aerosol/Water vapour/Cirrus) are 60m - often skipped for general analysis

        # The SCL (Scene Classification Layer) band is crucial for cloud masking.
        # For L1C, QA60 is used for cloud bits, but a direct SCL band (MSK_SCL_20m) is for L2A.

        # Pattern for 10m bands (B02, B03, B04, B08)
        pattern_10m = os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', '*_B0[2348].jp2')
        # Pattern for 20m bands (B05, B06, B07, B8A, B11, B12) and potentially SCL for L2A
        pattern_20m = os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', '*_B(0[567]|8A|1[12]).jp2')

        # For cloud masking in L1C, the QA60 band is often in the AUX_DATA directory or other QA files.
        # A more robust cloud mask for L1C would derive from B01, B08, B09, B10 and thresholds.
        # For simplicity, we'll try to find QA60 or related bands if available.
        qa_band_pattern = os.path.join(safe_dir, 'GRANULE', '*', 'QI_DATA', '*_QA60.jp2') # QA60 in QI_DATA for L1C
        scl_band_pattern = os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', '*_SCL_20m.jp2') # SCL for L2A

        current_files = glob.glob(pattern_10m)
        current_files.extend(glob.glob(pattern_20m))
        current_files.extend(glob.glob(qa_band_pattern)) # Add QA60
        current_files.extend(glob.glob(scl_band_pattern)) # Add SCL for L2A

        if current_files:
            jp2_files.extend(current_files)
        else:
            print(f"Warning: No JP2 image files found within expected path inside {os.path.basename(safe_dir)}.")
            print(f"Checked patterns: {pattern_10m}, {pattern_20m}, {qa_band_pattern}, {scl_band_pattern}")
            print("Please manually verify the internal structure of this .SAFE directory if issues persist.")

    # Filter out duplicates and ensure unique paths
    jp2_files = list(set(jp2_files))
    print(f"Found {len(jp2_files)} JP2 files for Sentinel-2 preprocessing in {roi_name}.")
    return jp2_files

def process_s2_image(s2_jp2_paths, roi_geometry):
    """
    Performs basic preprocessing steps on Sentinel-2 image bands.
    This includes band loading, resampling, simple cloud masking, and clipping.
    Note: This does NOT perform atmospheric correction for L1C data.
    """
    if not s2_jp2_paths:
        print("No JP2 files provided for processing.")
        return None

    # Group bands by product/granule
    product_groups = {}
    for path in s2_jp2_paths:
        # Extract product identifier and granule identifier from path
        # S2A_MSIL1C_20250705T053649_N0511_R005_T43SGT_20250705T072135.SAFE/GRANULE/L1C_T43SGT_A043503_20250705T054201
        match = re.search(r'(S2[AB]_MSIL[12]C_\d{8}T\d{6}_N\d{4}_R\d{3}_T\d{2}[A-Z]{3}_\d{8}T\d{6}.SAFE)/GRANULE/(L[12]C_T\d{2}[A-Z]{3}_A\d+_\d{8}T\d{6})', path)
        if match:
            product_id = match.group(1) # e.g., S2A_MSIL1C_...SAFE
            granule_id = match.group(2) # e.g., L1C_T43SGT_A...
            key = (product_id, granule_id)
            if key not in product_groups:
                product_groups[key] = []
            product_groups[key].append(path)
        else:
            print(f"Warning: Could not parse product/granule ID from path: {path}")

    processed_datasets = []

    for (product_id, granule_id), paths_in_granule in product_groups.items():
        print(f"\nProcessing product: {product_id}, Granule: {granule_id}")

        # Separate image bands from QA/SCL band
        image_band_paths = [p for p in paths_in_granule if not any(qa_str in p for qa_str in ['QA60', 'SCL_20m'])]
        qa_scl_band_paths = [p for p in paths_in_granule if any(qa_str in p for qa_str in ['QA60', 'SCL_20m'])]

        if not image_band_paths:
            print(f"No image bands found for {granule_id}. Skipping.")
            continue

        # Load selected bands (e.g., B02, B03, B04, B08, B11, B12)
        # We'll load them individually and then stack/resample.
        band_data = {}
        for band_path in image_band_paths:
            band_name_match = re.search(r'_B(\d{2}|8A)\.jp2$', os.path.basename(band_path))
            if band_name_match:
                band_name = band_name_match.group(1)
                try:
                    band_ds = rxr.open_rasterio(band_path, masked=True).squeeze()
                    # Ensure it's 2D (remove band dimension if present)
                    if band_ds.ndim == 3:
                        band_ds = band_ds.isel(band=0)
                    band_data[band_name] = band_ds.rio.set_band(band_name)
                except Exception as e:
                    print(f"Error opening band {band_name} from {band_path}: {e}")
            else:
                print(f"Could not extract band name from {os.path.basename(band_path)}")

        if not band_data:
            print(f"No valid image bands loaded for {granule_id}. Skipping.")
            continue

        # Identify a common CRS and resolution (e.g., 10m)
        # Use B04 as reference for 10m CRS and transform
        reference_band_name = '04' # Or '02', '03', '08'
        if reference_band_name not in band_data:
            print(f"Warning: Reference band {reference_band_name} (Red) not found. Cannot determine common resolution. Skipping granule.")
            continue

        # Reproject all bands to a common CRS and resolution (e.g., 10m)
        print("Resampling and reprojecting bands to 10m resolution...")
        resampled_bands = []
        for band_name, ds in band_data.items():
            try:
                # Reproject to reference band's CRS and resolution
                resampled_ds = ds.rio.reproject(
                    band_data[reference_band_name].rio.crs,
                    resolution=band_data[reference_band_name].rio.resolution(),
                    resampling=rasterio.enums.Resampling.average # Or nearest, bilinear
                )
                resampled_bands.append(resampled_ds)
            except Exception as e:
                print(f"Error resampling band {band_name}: {e}. Skipping band.")

        if not resampled_bands:
            print(f"No bands successfully resampled for {granule_id}. Skipping.")
            continue

        # Stack bands into a single DataArray
        print("Stacking bands...")
        # Use xr.concat to stack along a new 'band' dimension
        stacked_data = xr.concat(resampled_bands, dim='band')
        # Set band names as coordinates
        stacked_data['band'] = [ds.name for ds in resampled_bands]
        print(f"Stacked {len(resampled_bands)} bands.")

        # --- Cloud Masking ---
        # This is a basic cloud masking using QA60 (for L1C) or SCL (for L2A).
        # For L1C, QA60 bitmask:
        # Bit 10: Saturated or defective pixel
        # Bit 11: Cloud high confidence
        # Bit 12: Cloud medium confidence
        # Bit 14: Cirrus (L1C only)

        mask = None
        if qa_scl_band_paths:
            print("Applying cloud mask...")
            qa_scl_path = qa_scl_band_paths[0] # Take the first found QA/SCL band
            try:
                qa_scl_ds = rxr.open_rasterio(qa_scl_path, masked=True).squeeze()
                # Reproject QA/SCL to match image data CRS and resolution
                qa_scl_ds = qa_scl_ds.rio.reproject_match(stacked_data)

                if 'QA60' in os.path.basename(qa_scl_path): # For L1C QA60
                    # Bits 10 (cloud high conf), 11 (cloud medium conf), 14 (cirrus)
                    # Create mask: True where cloudy/cirrus, False otherwise
                    cloud_mask = (qa_scl_ds.values & (1 << 10)).astype(bool) | \
                                 (qa_scl_ds.values & (1 << 11)).astype(bool) | \
                                 (qa_scl_ds.values & (1 << 14)).astype(bool)
                    mask = cloud_mask
                    print("QA60 cloud mask applied.")
                elif 'SCL_20m' in os.path.basename(qa_scl_path): # For L2A SCL
                    # SCL values: 3 (cloud shadow), 8 (cloud medium), 9 (cloud high), 10 (cirrus)
                    cloud_values = [3, 8, 9, 10]
                    mask = np.isin(qa_scl_ds.values, cloud_values)
                    print("SCL cloud mask applied (for L2A data).")
                else:
                    print("Unknown QA/SCL band type. Skipping cloud masking.")

            except Exception as e:
                print(f"Error applying cloud mask from {os.path.basename(qa_scl_path)}: {e}")
                import traceback
                traceback.print_exc() # Print full traceback for debugging

        if mask is not None:
            # Apply mask to all bands in the stacked data
            # Set masked pixels to NaN
            # Ensure mask matches shape of data (broadcast if necessary)
            stacked_data = stacked_data.where(~xr.DataArray(mask, dims=stacked_data.dims, coords=stacked_data.coords), np.nan)
            print("Cloudy pixels set to NaN.")
        else:
            print("No cloud mask applied.")

        # 6. Clipping to ROI
        print("Clipping to ROI...")
        roi_shapely = shape(roi_geometry)

        # Ensure the ROI geometry is in the same CRS as `stacked_data` before clipping.
        # rioxarray's clip should handle CRS differences if source CRS of shape is known.
        clipped_data = stacked_data.rio.clip([roi_shapely], drop=True, crs=stacked_data.rio.crs)
        print("Clipped to ROI.")

        processed_datasets.append(clipped_data)

    if not processed_datasets:
        return None

    # If multiple granules or products were processed, you might want to mosaic them.
    # For simplicity, we'll return the first processed dataset or handle single granule output.
    # For BorderWatch, you might process granules individually or mosaic them for a larger area.
    if len(processed_datasets) == 1:
        return processed_datasets[0]
    else:
        print("Multiple datasets processed (from different granules/products). Consider mosaic step if needed.")
        # For now, return a list or the first one. For a real application, mosaic is critical.
        # E.g., return xr.concat(processed_datasets, dim='time').mean(dim='time') for a simple mosaic.
        return processed_datasets[0] # Just return the first for now.

if __name__ == "__main__":
    rois = load_rois(ROI_FILE)
    if not rois:
        print("No ROIs loaded. Exiting Sentinel-2 preprocessing.")
        exit()

    for roi in rois:
        roi_name = roi['properties'].get('name', 'Unnamed_ROI')
        s2_jp2_files = find_s2_jp2_files(roi_name)

        if not s2_jp2_files:
            print(f"No Sentinel-2 JP2 files found to process for ROI: {roi_name}. Skipping preprocessing for this ROI.")
            continue

        processed_roi_dir = os.path.join(PROCESSED_DATA_DIR, roi_name, 'SENTINEL-2')
        os.makedirs(processed_roi_dir, exist_ok=True)

        # Sentinel-2 processing typically happens per product/granule
        # We'll group files by product/granule and process each group
        product_granule_groups = {}
        for path in s2_jp2_files:
            match = re.search(r'(S2[AB]_MSIL[12]C_\d{8}T\d{6}_N\d{4}_R\d{3}_T\d{2}[A-Z]{3}_\d{8}T\d{6}.SAFE)/GRANULE/(L[12]C_T\d{2}[A-Z]{3}_A\d+_\d{8}T\d{6})', path)
            if match:
                product_key = match.group(1) # e.g., S2A_MSIL1C_...SAFE
                granule_key = match.group(2) # e.g., L1C_T43SGT_A...
                key = (product_key, granule_key)
                if key not in product_granule_groups:
                    product_granule_groups[key] = []
                product_granule_groups[key].append(path)
            else:
                print(f"Warning: Could not parse product/granule ID from path: {path} for grouping.")

        for (product_key, granule_key), paths_to_process in product_granule_groups.items():
            # Derive a unique output filename for each processed granule
            # Using granule key and the original product name without .SAFE suffix
            base_product_name = product_key.replace('.SAFE', '')
            output_filename = f"{base_product_name}_{granule_key}_preprocessed.tif"
            output_path = os.path.join(processed_roi_dir, output_filename)

            if os.path.exists(output_path):
                print(f"Skipping {output_filename} (already processed).")
                continue

            processed_s2_data = process_s2_image(paths_to_process, roi['geometry'])

            if processed_s2_data is not None:
                print(f"Saving processed Sentinel-2 image to: {output_path}")
                # Ensure the CRS is set on the xarray object before saving
                if processed_s2_data.rio.crs is None:
                    # Fallback to a common CRS if not already set by rioxarray reproject_match
                    print("Warning: CRS not set on processed_s2_data. Setting to EPSG:32643 (UTM) for saving.")
                    processed_s2_data = processed_s2_data.rio.write_crs("EPSG:32643")
                processed_s2_data.rio.to_raster(output_path)
                print("Saved successfully.")
            else:
                print(f"Failed to process Sentinel-2 data for product {base_product_name}, granule {granule_key}.")

    print("\nSentinel-2 Preprocessing complete for all ROIs.")