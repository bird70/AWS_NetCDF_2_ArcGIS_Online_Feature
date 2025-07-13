import os
import tempfile

import boto3
import geopandas as gpd
import pandas as pd


def lambda_handler(event, context, retain_temp_gpkg=False):
    """
    Step 2 Lambda: Download GeoPackage from S3, process with pandas/geopandas,
    and output final spatial layer.
    """
    # Clean up /tmp directory at the start unless retaining temp files
    import glob
    if not retain_temp_gpkg:
        for f in glob.glob('/tmp/*.gpkg'):
            try:
                os.remove(f)
            except Exception as e:
                print(f"Could not remove {f}: {e}")

    # Environment variables
    OUTPUT_S3_BUCKET = os.environ.get("OUTPUT_S3_BUCKET")
    # e.g. 'geopackages/temp_data_upload_xxx.gpkg'
    OUTPUT_S3_KEY = os.environ.get("OUTPUT_S3_KEY")
    # Name of riverlines layer in GPKG
    RIVERLINES_LAYER = os.environ.get("RIVERLINES_LAYER", "riverlines")
    # Name of model table in GPKG
    MODEL_TABLE = os.environ.get("MODEL_TABLE", "data")
    # Name of lookup table in GPKG
    LOOKUP_TABLE = os.environ.get("LOOKUP_TABLE", "lookup")
    INPUT_S3_KEY = os.environ.get("INPUT_S3_KEY")
    FINAL_OUTPUT_KEY = os.environ.get(
        "FINAL_OUTPUT_KEY", "geopackages/final_output.gpkg"
    )

    s3 = boto3.client("s3")

    # Download GeoPackage from S3 to /tmp
    import botocore
    import sqlite3
    
    try:
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp_file:
            s3.download_fileobj(
                OUTPUT_S3_BUCKET, OUTPUT_S3_KEY, tmp_file
            )
            gpkg_path = tmp_file.name
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "404" or error_code == "NoSuchKey":
            raise FileNotFoundError(
                f"GeoPackage not found in S3 bucket '{OUTPUT_S3_BUCKET}' "
                f"with key '{OUTPUT_S3_KEY}'. Check that the file exists "
                "and the key is correct."
            )
        else:
            raise

    try:
        with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as tmp_file_extract:
            s3.download_fileobj(
                OUTPUT_S3_BUCKET, INPUT_S3_KEY, tmp_file_extract
            )
            gpkg_path_extract = tmp_file_extract.name
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "404" or error_code == "NoSuchKey":
            raise FileNotFoundError(
                f"GeoPackage not found in S3 bucket '{OUTPUT_S3_BUCKET}' "
                f"with key '{INPUT_S3_KEY}'. Check that the file exists "
                "and the key is correct."
            )
        else:
            raise

    # Load tables/layers
    # Load tables/layers using sqlite3 for non-spatial tables
    conn = sqlite3.connect(gpkg_path)
    conn2 = sqlite3.connect(gpkg_path_extract)
    
    try:
        df_model = pd.read_sql_query(f"SELECT * FROM {MODEL_TABLE}", conn2)
        df_lookup = pd.read_sql_query(f"SELECT * FROM {LOOKUP_TABLE}", conn)
    except Exception:
        # If lookup is a layer, try geopandas
        df_model = gpd.read_file(gpkg_path_extract, layer=MODEL_TABLE)
        df_lookup = gpd.read_file(gpkg_path, layer=LOOKUP_TABLE)
    
    conn.close()
    conn2.close()
    # read riverlines
    gdf_riverlines = gpd.read_file(gpkg_path, layer=RIVERLINES_LAYER)

    # Ensure geometry column is named 'Shape' for SQL logic compatibility
    if "geometry" in gdf_riverlines.columns:
        gdf_riverlines = gdf_riverlines.rename(columns={"geometry": "Shape"})

    # 1. Merge and filter
    df_bools = pd.merge(
        df_model, df_lookup, left_on="nrch", right_on="nrch", how="left"
    )
    print("df_bools columns after merge:", df_bools.columns.tolist())
    print("First 5 rows of df_bools after merge:\n", df_bools.head())

    # Defensive: check for 'sum_bool_value_thsh' before filtering
    if "sum_bool_value_thsh" not in df_bools.columns:
        print("Warning: 'sum_bool_value_thsh' column missing after merge! Columns present:", df_bools.columns.tolist())
        # Optionally, raise or handle gracefully
        raise KeyError("'sum_bool_value_thsh' column missing after merge.")

    df_bools = df_bools[df_bools["sum_bool_value_thsh"] > 0][
        ["OBJECTID", "rchid", "nrthresholds", "sum_bool_value_thsh"]
    ]

    # 2. Merge with spatial layer
    gdf_max_sum = pd.merge(
        df_bools, gdf_riverlines, left_on="rchid", right_on="Top_reach", how="inner"
    )
    gdf_max_sum = gdf_max_sum[
        ["OBJECTID", "rchid", "sum_bool_value_thsh", "nrthresholds", "Shape"]
    ]

    # 3. Self-join to get max nrthresholds per rchid
    df_max = pd.merge(
        gdf_max_sum,
        gdf_max_sum,
        left_on="OBJECTID",
        right_on="OBJECTID",
        suffixes=("_A", "_B"),
    )
    df_max = df_max[df_max["sum_bool_value_thsh_A"] > 0]
    df_max = df_max[
        df_max["nrthresholds_A"]
        == df_max.groupby("rchid_A")["nrthresholds_B"].transform("max")
    ]
    df_max = df_max[
        ["rchid_A", "nrthresholds_A", "sum_bool_value_thsh_A"]
    ].drop_duplicates()
    df_max.columns = ["rchid", "nrthresholds", "sum_bool_value_thsh"]

    # 4. Merge with spatial layer again
    gdf_final = pd.merge(
        df_max, gdf_riverlines, left_on="rchid", right_on="Top_reach", how="inner"
    )
    print("gdf_final columns after merge:", gdf_final.columns.tolist())
    print("First 5 rows of gdf_final after merge:\n", gdf_final.head())

    # Ensure OBJECTID exists: use from riverlines if present, else create new
    if "OBJECTID" not in gdf_final.columns:
        print("OBJECTID not found in gdf_final, creating new sequential OBJECTID.")
        gdf_final["OBJECTID"] = range(1, len(gdf_final) + 1)

    required_cols = ["OBJECTID", "rchid", "sum_bool_value_thsh", "nrthresholds", "Shape"]
    gdf_final = gdf_final[required_cols]

    # Convert 'Shape' back to geometry for GeoPackage output
    # Defensive: ensure gdf_riverlines has a valid geometry and CRS before using its CRS
    if hasattr(gdf_riverlines, "geometry") and gdf_riverlines.geometry.name and gdf_riverlines.crs is not None:
        crs_to_use = gdf_riverlines.crs
    else:
        print("Warning: gdf_riverlines has no valid CRS. Output GeoDataFrame will have no CRS.")
        crs_to_use = None
    gdf_final = gpd.GeoDataFrame(gdf_final, geometry="Shape", crs=crs_to_use)

    # Write final output to GeoPackage in /tmp
    final_gpkg_path = os.path.join(tempfile.gettempdir(), "final_output.gpkg")
    gdf_final.to_file(final_gpkg_path, layer="final_layer", driver="GPKG")

    # Upload final GeoPackage to S3
    with open(final_gpkg_path, "rb") as f:
        s3.upload_fileobj(f, OUTPUT_S3_BUCKET, FINAL_OUTPUT_KEY)

    # === ArcGIS Online Upload and Feature Layer Update ===
    import time
    import uuid

    from arcgis.features import FeatureLayer
    from arcgis.gis import GIS

    AGOURL = os.environ["AGOURL"]
    AGOUSERNAME = os.environ["AGOUSERNAME"]
    AGOPASSWORD_PARAM = os.environ["AGOPASSWORD"]
    HOSTED_FEATURE_LAYER_URL = os.environ["HOSTED_FEATURE_LAYER_URL"]
    TEMP_ITEM_ID_S3_KEY = os.environ.get(
        "TEMP_ITEM_ID_S3_KEY", "geopackages/last_temp_item_id.txt"
    )

    # Retrieve AGOPASSWORD from SSM
    ssm_client = boto3.client("ssm")
    response = ssm_client.get_parameter(Name=AGOPASSWORD_PARAM, WithDecryption=True)
    AGOPASSWORD = response["Parameter"]["Value"]

    # Connect to ArcGIS Online
    gis = GIS(AGOURL, AGOUSERNAME, AGOPASSWORD)
    feature_layer = FeatureLayer(HOSTED_FEATURE_LAYER_URL, gis=gis)

    # Delete previous temporary item if exists
    try:
        temp_item_id = None
        try:
            temp_item_id_obj = s3.get_object(
                Bucket=OUTPUT_S3_BUCKET, Key=TEMP_ITEM_ID_S3_KEY
            )
            temp_item_id = temp_item_id_obj["Body"].read().decode("utf-8").strip()
        except Exception:
            temp_item_id = None
        if temp_item_id:
            print("Deleting previous temporary ArcGIS Online item: " f"{temp_item_id}")
            try:
                item = gis.content.get(temp_item_id)
                if item:
                    item.delete()
                    print("Previous temporary item deleted.")
            except Exception as e:
                print(f"Error deleting previous item: {e}")
    except Exception as e:
        print(f"Error checking for previous temp item: {e}")

    # Upload new GeoPackage as an item
    print("Uploading new GeoPackage to ArcGIS Online...")
    unique_title = f"temp_data_upload_{uuid.uuid4().hex}"
    geopackage_item = gis.content.add(
        {
            "title": unique_title,
            "type": "GeoPackage",
            "tags": "data upload, automation",
            "description": (
                "Temporary GeoPackage file for updating a " "hosted feature layer."
            ),
        },
        data=final_gpkg_path,
    )
    print(f"GeoPackage uploaded. Item ID: {geopackage_item.id}")

    # Save new item ID to S3 for next run's cleanup
    s3.put_object(
        Bucket=OUTPUT_S3_BUCKET,
        Key=TEMP_ITEM_ID_S3_KEY,
        Body=geopackage_item.id.encode("utf-8"),
    )

    # Truncate the feature layer
    print("Truncating the feature layer...")
    # truncate_result = feature_layer.manager.truncate()
    # if truncate_result.get("success"):
    #     print("Feature layer truncated successfully.")
    # else:
    #     print("Failed to truncate the feature layer.")

    # Append data from GeoPackage to the feature layer
    print("Appending data from GeoPackage to the feature layer...")
    append_result = feature_layer.append(
        item_id=geopackage_item.id,
        upload_format="geoPackage",
        upsert=False,
        future=True,
    )
    print("Append operation started.")
    # Optionally, wait for completion (poll status)
    if hasattr(append_result, "status"):
        for _ in range(30):
            status = append_result.status()
            print(f"Append status: {status}")
            if status.get("status") == "Completed":
                break
            time.sleep(10)

    # Delete the temporary ArcGIS Online item
    geopackage_item.delete()
    print("Temporary GeoPackage item deleted from ArcGIS Online.")

    # After uploading final GeoPackage to S3, clean up temp files unless retaining
    if not retain_temp_gpkg:
        for f in [gpkg_path, gpkg_path_extract, final_gpkg_path]:
            try:
                os.remove(f)
            except Exception as e:
                print(f"Could not remove {f}: {e}")

    return {
        "statusCode": 200,
        "body": (
            f"Final spatial layer written to s3://{OUTPUT_S3_BUCKET}/"
            f"{FINAL_OUTPUT_KEY} and uploaded to ArcGIS Online."
        ),
    }


# if __name__ == "__main__":
#     # Example: set environment variables for local test
#     import json

#     os.environ["OUTPUT_S3_BUCKET"] = "s3-lambda-stack-prd-output-bucket-prod"
#     os.environ["OUTPUT_S3_KEY"] = "your-input-geopackage.gpkg"
#     os.environ.get("INPUT_S3_KEY") = "extract_geopackage.gpkg"
#     os.environ["FINAL_OUTPUT_KEY"] = "geopackages/final_output.gpkg"
#     os.environ["RIVERLINES_LAYER"] = "riverlines"
#     os.environ["MODEL_TABLE"] = "data"
#     os.environ["LOOKUP_TABLE"] = "lookup"
#     os.environ["AGOURL"] = "https://yourorg.maps.arcgis.com"
#     os.environ["AGOUSERNAME"] = "USERNAME"
#     os.environ["AGOPASSWORD"] = "/AGOPASSWORD"
#     os.environ["HOSTED_FEATURE_LAYER_URL"] = (
#         "https://services3.arcgis.com/XXXXX/arcgis/rest/services/final_output/FeatureServer/0"
#     )
#     # Optionally set TEMP_ITEM_ID_S3_KEY if you want to test item cleanup
#     # os.environ["TEMP_ITEM_ID_S3_KEY"] = "geopackages/last_temp_item_id.txt"

#     # Simulate a Lambda event (edit as needed)
#     event = {}
#     context = None
#     # Optionally, load event from a file: event = json.load(open('event.json'))
#     result = lambda_handler(event, context)
#     print(json.dumps(result, indent=2))
