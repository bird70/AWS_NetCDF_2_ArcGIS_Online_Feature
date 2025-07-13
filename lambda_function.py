import concurrent.futures
import json
import logging
import os
import uuid  # For generating unique filenames
from datetime import datetime as dt

import boto3
import geopandas as gpd
import pandas as pd
import s3fs
from arcgis.features import FeatureLayer, FeatureLayerCollection
from arcgis.gis import GIS
from arcgis.layers import Service
from netCDF4 import Dataset, num2date
from shapely.geometry import Point
import sqlite3
import tempfile

HOSTED_FEATURE_LAYER_URL = os.environ["HOSTED_FEATURE_LAYER_URL"]
SECOND_FEATURE_LAYER_URL = os.environ["SECOND_FEATURE_LAYER_URL"]
# Get the password from environment variables
MyPASSWORD = os.environ["AGOPASSWORD"]
AGOURL = os.environ["AGOURL"]
AGOUSERNAME = os.environ["AGOUSERNAME"]

s3_client = boto3.client("s3")
# Try to get the password from AWS SSM Parameter Store

ssm_client = boto3.client("ssm")
response = ssm_client.get_parameter(Name=MyPASSWORD, WithDecryption=True)
AGOPASSWORD = response["Parameter"]["Value"]

def convert_to_datetime(cftime_obj):
    """Convert a cftime or datetime object to a standard datetime object."""
    try:
        if isinstance(cftime_obj, dt):
            return cftime_obj
        return dt(
            cftime_obj.year,
            cftime_obj.month,
            cftime_obj.day,
            cftime_obj.hour,
            cftime_obj.minute,
            cftime_obj.second,
        )
    except AttributeError as e:
        print(f"Error converting cftime_obj: {cftime_obj}, {e}")
        raise


def process_time_step(
    time,
    data,
    nrch,
    rchid,
    streamorder,
    absolute_values,
    absolute_values_25th,
    absolute_values_5th,
    absolute_values_75th,
    absolute_values_95th,
    absolute_values_median,
    relative_thresholds_10yr,
    relative_thresholds_20yr,
    relative_thresholds_2yr,
    relative_thresholds_5yr,
    relative_values,
    relative_values_25th,
    relative_values_5th,
    relative_values_75th,
    relative_values_95th,
    relative_values_median,
):
    """Process a single time step and return rows for the DataFrame."""
    rows = []
    for j in range(len(rchid)):
        row = {
            "time_stamp_date": time,  # Keep as a datetime object
            "nrch": (
                nrch[j] if isinstance(nrch, list) else nrch
            ),  # Ensure nrch is included
            "rchid": int(rchid[j]),
            "streamorder": int(streamorder[j]),
            "absolutevalues": float(absolute_values[j]),
            "absolutevalues25thpercentile": float(absolute_values_25th[j]),
            "absolutevalues5thpercentile": float(absolute_values_5th[j]),
            "absolutevalues75thpercentile": float(absolute_values_75th[j]),
            "absolutevalues95thpercentile": float(absolute_values_95th[j]),
            "absolutevaluesmedian": float(absolute_values_median[j]),
            "relativevalues": float(relative_values[j]),
            "relativevalues25thpercentile": float(relative_values_25th[j]),
            "relativevalues5thpercentile": float(relative_values_5th[j]),
            "relativevalues75thpercentile": float(relative_values_75th[j]),
            "relativevalues95thpercentile": float(relative_values_95th[j]),
            "relativevaluesmedian": float(relative_values_median[j]),
            "relative_thresholds_10yr": float(relative_thresholds_10yr[j]),
            "relative_thresholds_20yr": float(relative_thresholds_20yr[j]),
            "relative_thresholds_2yr": float(relative_thresholds_2yr[j]),
            "relative_thresholds_5yr": float(relative_thresholds_5yr[j]),
        }
        rows.append(row)
    return rows


def upload_geopackage_to_arcgis(
    gis, geopackage_path, s3_bucket, s3_key, feature_layer, overwrite=True
):
    """Upload a GeoPackage to ArcGIS Online and update the hosted feature layer."""
    try:
        print("Uploading GeoPackage to ArcGIS Online...")
        root_folder = gis.content.folders.get()
        # geopackage_item = gis.content.add(
        geopackage_item = root_folder.add(
            {
                "title": geopackage_path.split("/")[-1],
                "type": "GeoPackage",
                "tags": "data upload, automation",
                "description": "Temporary GeoPackage file for updating a hosted feature layer.",
            },
            file=geopackage_path,
        ).result()
        print(f"GeoPackage uploaded successfully. Item ID: {geopackage_item.id}")
    except Exception as e:
        print(f"Error during GeoPackage upload and update: {e}")
        raise

    try:
        print("Updating the hosted feature layer...")
        print(f"GeoPackage uploaded successfully. Item ID: {geopackage_item.id}")

        # Step 4: Append or Overwrite Data in the Feature Layer
        print("Updating the hosted feature layer...")
        if overwrite:
            # Overwrite the entire feature layer
            result = feature_layer.manager.fromitem(geopackage_item.id)
        else:
            # Append data to the feature layer
            result = feature_layer.append(
                item_id=geopackage_item.id,
                upload_format="geoPackage",
                upsert=False,  # Set to True if you want to update existing records
                future=True,  # Set to True if you want to use the asynchronous process
            )

        if result:
            print("Data being updated in the feature layer.")
        else:
            print(f"Failed to update data. Response: {result}")

    except Exception as e:
        print(f"Error during GeoPackage upload and update: {e}")
        raise

    return geopackage_item


# Permanently delete an item from ArcGIS Online
def delete_item_permanently(item, gis):
    try:
        org_user = gis.users.search(AGOUSERNAME)[0]
        for r_item in org_user.recyclebin.content:
            print(
                f"{r_item.properties['title']:15} {r_item.properties['type']:22}{type(r_item)}"
            )
            if r_item.properties["title"].startswith(f"{item.title}"):
                # if (r_item.properties['type']:22) == "Map Service":
                r_item.delete()
                print(f"Item deleted from recycle bin: {r_item.properties['title']}")
                logging.info(
                    f"Item deleted from recycle bin: {r_item.properties['title']}"
                )

    except Exception as e:
        print(f"Failed to delete item {item.title} permanently: {e}")


# Delete the previous temporary GeoPackage item from ArcGIS Online
def delete_previous_item_from_agol(gis, s3_bucket, s3_key):
    """Delete the previous temporary GeoPackage item from ArcGIS Online."""
    import json

    import boto3

    s3_client = boto3.client("s3")

    try:
        # Download the metadata file from S3
        s3_metadata_key = f"{s3_key}/item_metadata.json"
        metadata_file = os.path.join(tempfile.gettempdir(), "item_metadata.json")
        print(f"Downloading metadata file from S3: {s3_metadata_key}")
        s3_client.download_file(s3_bucket, s3_metadata_key, metadata_file)

        # Parse the metadata file
        with open(metadata_file, "r") as f:
            metadata = json.load(f)

        # Iterate over the metadata entries
        for key, value in metadata.items():
            item_id = value.get("item_id")
            if not item_id:
                print(
                    f"No item ID found for {key} in metadata file. Skipping deletion."
                )
                continue

            # Delete the item from ArcGIS Online
            print(f"Deleting item from ArcGIS Online: {item_id.title}")
            # item_to_delete = gis.content.search(query=f"title:{item_id.title}")
            item_to_delete = gis.content.get(item_id)
            print(f"Deleting item from ArcGIS Online: {item_to_delete.title}")

            if item_to_delete:
                item_to_delete.delete()
                print("Item deleted successfully.")
                try:
                    # Also delete item from the recycle bin
                    delete_item_permanently(item_to_delete, gis)
                except Exception as e:
                    print(f"Failure when trying to delete permanently: {e}")

            else:
                print("Item not found in ArcGIS Online.")
            

        # Delete the metadata file from S3
        print(f"Deleting metadata file from S3: {s3_metadata_key}")
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_metadata_key)
        print("Metadata file deleted from S3 successfully.")

    except Exception as e:
        print(f"Error deleting previous items from ArcGIS Online: {e}")




def aggregate_table(data, group_by_column, column_filter):
    """Aggregate data by a specific column and calculate maximum values for filtered columns."""
    # Identify columns of interest based on the filter
    columns_of_interest = [col for col in data.columns if column_filter in col.lower()]

    # Ensure 'time_stamp_date' is included in the aggregation
    if "time_stamp_date" in data.columns:
        columns_of_interest.append("time_stamp_date")

    # Perform aggregation: calculate the maximum for each group
    aggregated_data = data.groupby(group_by_column, as_index=False)[
        columns_of_interest
    ].max()

    return aggregated_data


def clean_and_filter_data(data, invalid_values, datetime_column):
    """Clean and filter data by removing invalid values and ensuring datetime format."""
    data[datetime_column] = pd.to_datetime(data[datetime_column], errors="coerce")
    data = data.dropna(subset=[datetime_column])
    return data[~data.isin(invalid_values).any(axis=1)]


def join_geopackage_tables(geopackage_path, layer_a, layer_b, join_key_a, join_key_b):
    """Perform a join between two layers in a GeoPackage."""
    # Ensure layer_b is a valid string or integer
    if not isinstance(layer_b, (str, int)):
        raise ValueError(
            f"Invalid layer_b: {layer_b}. It must be a string or integer representing a layer name or index."
        )

    data_a = gpd.read_file(geopackage_path, layer=layer_a)
    data_b = gpd.read_file(geopackage_path, layer=layer_b)
    joined_data = data_a.merge(
        data_b, left_on=join_key_a, right_on=join_key_b, how="inner"
    )
    return joined_data


def write_dataframe_to_geopackage(
    df, geopackage_path, table_name, add_dummy_geometry=True, overwrite=True
):
    """Write a DataFrame to a GeoPackage table, ensuring the geometry column is named 'SHAPE'.
    If overwrite is True, the GeoPackage file is deleted if it exists. If False, the new layer is appended.
    """
    from shapely.geometry import Point

    # Ensure the input is a GeoDataFrame
    if not isinstance(df, gpd.GeoDataFrame):
        if "SHAPE" in df.columns:
            df = gpd.GeoDataFrame(df, geometry="SHAPE")
        elif "geom" in df.columns:
            df = gpd.GeoDataFrame(df, geometry="geom")
        elif "geometry" in df.columns:
            df = gpd.GeoDataFrame(df, geometry="geometry")
        elif add_dummy_geometry:
            print("Adding dummy geometries to the DataFrame...")
            df["geometry"] = [Point(0, 0) for _ in range(len(df))]
            df = gpd.GeoDataFrame(df, geometry="geometry")
        else:
            raise ValueError(
                "The input DataFrame must be a GeoDataFrame with a valid geometry or SHAPE column."
            )

    # Handle multiple geometry columns
    geometry_columns = df.columns[df.dtypes == "geometry"]
    if len(geometry_columns) > 1:
        print(
            "GeoDataFrame contains multiple geometry columns. Dropping additional geometry columns..."
        )
        df = (
            df.set_geometry("geometry", inplace=False)
            if "geometry" in geometry_columns
            else df.set_geometry(geometry_columns[0], inplace=False)
        )
        df = df.loc[:, ~df.columns.duplicated()]  # Drop duplicate columns

    # Rename the geometry column to 'SHAPE'
    if df.geometry.name != "SHAPE":
        print("Renaming geometry column to 'SHAPE'...")
        df = df.rename_geometry("SHAPE")

    # Only delete the GeoPackage if overwrite is True
    if overwrite and os.path.exists(geopackage_path):
        print(f"GeoPackage file '{geopackage_path}' already exists. Deleting it...")
        os.remove(geopackage_path)

    # Write the GeoDataFrame to the GeoPackage (append if file exists and overwrite is False)
    mode = "w" if overwrite or not os.path.exists(geopackage_path) else "a"
    df.to_file(geopackage_path, layer=table_name, driver="GPKG")



def join_geopackage_tables_in_memory(
    geopackage_path, layer_a, cleaned_data, join_key_a, join_key_b, join_type="inner"
):
    """Perform a join between an in-memory DataFrame and a layer from a GeoPackage."""
    # Load the existing table from the GeoPackage
    data_a = gpd.read_file(geopackage_path, layer=layer_a)

    # Perform the join in-memory based on the specified join type
    joined_data = data_a.merge(
        cleaned_data, left_on=join_key_a, right_on=join_key_b, how=join_type
    )

    # For inner joins, ensure only one record per 'rchid' (or 'Top_reach')
    if join_type == "inner":
        joined_data = joined_data.drop_duplicates(subset=[join_key_a])

    return joined_data


def process_netCDF_file(s3_path):
    """Process the NetCDF file and return a DataFrame."""
    try:
        print(f"Opening NetCDF file from S3 path: {s3_path}")
        fs = s3fs.S3FileSystem()
        with fs.open(s3_path, "rb") as f:
            dataset = Dataset("dummy", mode="r", memory=f.read())
        print("NetCDF file loaded successfully.")
    except Exception as e:
        print(f"Error loading NetCDF file: {e}")
        raise e

    print("Processing data...")
    data = {}

    # Extract the time variable
    time_var = dataset.variables["time"]
    time_values = num2date(time_var[:], units=time_var.units)

    # Convert time values to standard datetime objects
    time_values = [convert_to_datetime(time) for time in time_values]

    # Extract non-time-dependent variables
    for var in ["rchid", "streamorder"]:
        if var in dataset.variables:
            data[var] = dataset.variables[var][:].tolist()
        else:
            raise KeyError(f"Variable '{var}' not found in the NetCDF file.")

    # Extract the nrch dimension
    if "nrch" in dataset.variables:
        nrch = dataset.variables["nrch"][:].tolist()
    elif "nrch" in dataset.dimensions:
        nrch = len(dataset.dimensions["nrch"])
    else:
        raise KeyError("Dimension 'nrch' not found in the NetCDF file.")

    # Extract time-dependent variables
    for var in [
        "absoluteValues",
        "relativeValues",
        "absoluteValues25thPercentile",
        "absoluteValues5thPercentile",
        "absoluteValues75thPercentile",
        "absoluteValues95thPercentile",
        "absoluteValuesMedian",
        "relativeValues25thPercentile",
        "relativeValues5thPercentile",
        "relativeValues75thPercentile",
        "relativeValues95thPercentile",
        "relativeValuesMedian",
    ]:
        if var in dataset.variables:
            var_data = dataset.variables[var][:]
            print(f"Variable '{var}' shape: {var_data.shape}")
            if var_data.shape[0] != len(time_values):
                raise ValueError(
                    f"Variable '{var}' does not have the same time dimension as 'time'."
                )
            data[var] = var_data
        else:
            raise KeyError(f"Variable '{var}' not found in the NetCDF file.")

    # Extract thresholds
    for var in [
        "relative_thresholds_10yr",
        "relative_thresholds_20yr",
        "relative_thresholds_2yr",
        "relative_thresholds_5yr",
    ]:
        if var in dataset.variables:
            var_data = dataset.variables[var][:]
            print(f"Variable '{var}' shape: {var_data.shape}")
            if var_data.shape[0] != len(data["rchid"]):
                raise ValueError(
                    f"Variable '{var}' does not have the same spatial dimension as 'rchid'."
                )
            data[var] = var_data
        else:
            raise KeyError(f"Variable '{var}' not found in the NetCDF file.")

    # Step 3: Process data in parallel
    print("Processing data in parallel...")
    rows = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_time_step,
                time_values[i],
                data,
                nrch,
                data["rchid"],
                data["streamorder"],
                data["absoluteValues"][i],
                data["absoluteValues25thPercentile"][i],
                data["absoluteValues5thPercentile"][i],
                data["absoluteValues75thPercentile"][i],
                data["absoluteValues95thPercentile"][i],
                data["absoluteValuesMedian"][i],
                data["relative_thresholds_10yr"],
                data["relative_thresholds_20yr"],
                data["relative_thresholds_2yr"],
                data["relative_thresholds_5yr"],
                data["relativeValues"][i],
                data["relativeValues25thPercentile"][i],
                data["relativeValues5thPercentile"][i],
                data["relativeValues75thPercentile"][i],
                data["relativeValues95thPercentile"][i],
                data["relativeValuesMedian"][i],
            )
            for i in range(len(time_values))
        ]
        for future in concurrent.futures.as_completed(futures):
            rows.extend(future.result())

    # Step 4: Create a pandas DataFrame
    print("Creating pandas DataFrame...")
    df = pd.DataFrame(rows)
    df["time_stamp_date"] = pd.to_datetime(df["time_stamp_date"])
    print(f"DataFrame created with shape: {df.shape}")

    return df


def extract_threshold_summary_from_netcdf(s3_path):
    """
    Extracts the sum_bool_value_thsh variable from the NetCDF file and returns a DataFrame
    grouped by nrch and nrthresholds, for the '0-48' timewindow (index 3).
    """
    import pandas as pd
    import s3fs
    from netCDF4 import Dataset
    from shapely.geometry import Point

    fs = s3fs.S3FileSystem()
    with fs.open(s3_path, "rb") as f:
        dataset = Dataset("dummy", mode="r", memory=f.read())

    # Always use index 3 for the '0-48' timewindow
    timewindow_index = 3  # 0-based index for the 4th timewindow

    # Extract variables
    sum_bool_value_thsh = dataset.variables[
        "sum_bool_value_thsh"
    ]  # shape: (nrthresholds, nrch, timewindows)
    nrch = (
        dataset.variables["nrch"][:]
        if "nrch" in dataset.variables
        else range(sum_bool_value_thsh.shape[1])
    )
    nrthresholds = (
        dataset.variables["nrthresholds"][:]
        if "nrthresholds" in dataset.variables
        else range(sum_bool_value_thsh.shape[0])
    )

    # Flatten to long format, only for timewindow_index == 3
    records = []
    for i, threshold in enumerate(nrthresholds):
        for j, reach in enumerate(nrch):
            records.append(
                {
                    "nrch": int(reach),
                    "nrthresholds": int(threshold),
                    "sum_bool_value_thsh": float(
                        sum_bool_value_thsh[i, j, timewindow_index]
                    ),
                    "geometry": Point(0, 0),
                }
            )
    df = pd.DataFrame(records)
    return df


# lambda_handler function

def lambda_handler(event, context):
    # Enable logging for ArcGIS API
    logging.basicConfig(level=logging.INFO)

    print(f"Connecting to ArcGIS Online {AGOURL}")
    # Initialize the GIS connection
    gis = GIS(AGOURL, AGOUSERNAME, AGOPASSWORD)
    s3_client = boto3.client("s3")

    print(f"Connected to ArcGIS Online {AGOURL}")
    print(f"Feature Layer URL: {HOSTED_FEATURE_LAYER_URL}")

    # Step 0: Read the NetCDF file from S3
    print("Reading NetCDF file from S3...")

    # Get the bucket name and object key from the event
    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_key = event["Records"][0]["s3"]["object"]["key"]

    print(f"S3 bucket: {s3_bucket}")
    print(f"S3 key: {s3_key}")

    s3_path = f"s3://{s3_bucket}/{s3_key}"

    # Step 1: Delete the previous temporary GPKG item from ArcGIS Online
    delete_previous_item_from_agol(gis, s3_bucket, s3_key)

    # Step 2: Process the NetCDF file
    print("Processing NetCDF file...")
    s3_path = f"s3://{s3_bucket}/{s3_key}"
    df = process_netCDF_file(s3_path)

    # Step 6: Aggregate the table created in the Lambda function
    print("Aggregating data...")
    aggregated_data = aggregate_table(
        df, ["rchid", "streamorder"], "relativevalues95thpercentile"
    )
    print("Data aggregated successfully.")

    # Step 7: Clean and filter the aggregated data
    print("Cleaning and filtering data...")
    cleaned_data = clean_and_filter_data(
        aggregated_data, [-888, 888, 999], "time_stamp_date"
    )
    print("Data cleaned and filtered successfully.")

    # Step 8: Retrieve the reference GeoPackage from S3 and save it under a distinct name
    print("Retrieving reference GeoPackage from S3...")
    reference_s3_key = "REC1_Geopackage/a_gpkg.gpkg"
    s3_bucket_download = "s3-lambda-stack-prd-input-bucket-prod"  # Static bucket name from test event
    reference_local_path = os.path.join(
        tempfile.gettempdir(), "reference_geopackage.gpkg"
    )

    # Check if the file already exists locally
    if os.path.exists(reference_local_path):
        print(f"Reference GeoPackage already exists locally at: {reference_local_path}")
        logging.info(
            f"Reference GeoPackage already exists locally at: {reference_local_path}"
        )
    else:
        # Ensure the file is not locked during download
        try:
            s3_client.download_file(
                s3_bucket_download, reference_s3_key, reference_local_path
            )
            print(
                f"Reference GeoPackage retrieved and saved to: {reference_local_path}"
            )
            logging.info(
                f"Reference GeoPackage retrieved and saved to: {reference_local_path}"
            )
        except Exception as e:
            print(f"Error downloading the reference GeoPackage: {e}")
            logging.error(f"Error downloading the reference GeoPackage: {e}")
            raise

    # Step 9: Perform the join logic for the first GeoPackage using raw data
    print(
        "Performing join between riverlines and raw data in-memory for the first GeoPackage..."
    )
    cleaned_raw_data = clean_and_filter_data(df, [-888, 888, 999], "time_stamp_date")
    joined_raw_data = join_geopackage_tables_in_memory(
        reference_local_path,
        "R1_Riverlines_SimplifyLine",
        cleaned_raw_data,
        "Top_reach",
        "rchid",
        join_type="right",
    )
    print("Join operation completed successfully for the first GeoPackage.")

    # Reduce precision for numeric columns before writing first GeoPackage
    cols_to_round_1 = [
        col
        for col in joined_raw_data.columns
        if col.startswith("absolutevalues") or col.startswith("relativevalues")
    ]
    joined_raw_data[cols_to_round_1] = joined_raw_data[cols_to_round_1].round(2)

    # Step 10: Write the joined raw data to a new GeoPackage for the first join
    print("Creating a new GeoPackage for the first join...")
    first_geopackage_path = os.path.join(
        tempfile.gettempdir(), "first_join_geopackage.gpkg"
    )
    print("Writing joined raw data to the first GeoPackage...")
    output_table_name_first = "joined_raw_riverlines"
    write_dataframe_to_geopackage(
        joined_raw_data, first_geopackage_path, output_table_name_first, False, True
    )
    print(
        f"Joined raw data written to the first GeoPackage as layer '{output_table_name_first}'."
    )

    # Extract threshold summary from NetCDF and write to the first GeoPackage
    print(
        "Extracting threshold summary for timewindows == 3 and writing to GeoPackage..."
    )
    threshold_summary_df = extract_threshold_summary_from_netcdf(s3_path)
    output_s3_key = os.environ.get("OUTPUT_S3_KEY")
    extract_geopackage_path = os.path.join(tempfile.gettempdir(), output_s3_key)
    if not threshold_summary_df.empty:
        try:
            write_dataframe_to_geopackage(
                threshold_summary_df,
                extract_geopackage_path,
                "data",  # Correct table name
                add_dummy_geometry=True,
                overwrite=False,
            )
            print(
                "Threshold summary table written to the first GeoPackage as 'threshold_summary'."
            )
        except Exception as e:
            print(f"Error writing threshold summary table to GeoPackage: {e}")
            logging.error(f"Error writing threshold summary table to GeoPackage: {e}")
    else:
        print("No threshold summary table written (no timewindows == 3 found).")

    # Step 11: Upload the GeoPackage to an output S3 bucket
    output_s3_bucket = os.environ.get(
        "OUTPUT_S3_BUCKET"
    )  # Set this env var in Lambda config
    output_s3_key = os.environ.get("OUTPUT_S3_KEY")  # Set this env var in Lambda config
    # output_s3_key = f"geopackages/{os.path.basename(unique_filename)}"
    try:
        s3_client = boto3.client("s3")
        print(
            f"Uploading GeoPackage to S3 bucket: {output_s3_bucket}, key: {output_s3_key}"
        )
        # s3_client.upload_file(first_geopackage_path, output_s3_bucket, output_s3_key)
        s3_client.upload_file(extract_geopackage_path, output_s3_bucket, output_s3_key)

        print("GeoPackage uploaded to output S3 bucket successfully.")
    except Exception as e:
        print(f"Error uploading GeoPackage to output S3 bucket: {e}")

    # Ensure the correct GeoPackage is used for the first upload
    # Step 11: Upload the first GeoPackage and update ArcGIS Online
    print("Uploading the first GeoPackage and updating ArcGIS Online...")

    # Truncate the feature layer before updating
    print("Truncating the feature layer...")
    feature_layer = Service(HOSTED_FEATURE_LAYER_URL)
    truncate_result = feature_layer.manager.truncate()
    if truncate_result["success"]:
        print("Feature layer truncated successfully.")
    else:
        print("Failed to truncate the feature layer.")

    root_folder = gis.content.folders.get()
    # Call the function to upload the first GeoPackage and update ArcGIS Online
    geopackage_item = upload_geopackage_to_arcgis(
        gis,
        first_geopackage_path,  # Use the correct GeoPackage path
        s3_bucket,
        s3_key,
        feature_layer,
        overwrite=False,
    )

    print(
        "First GeoPackage uploaded and ArcGIS Online updated successfully. Item ID: {geopackage_item.id}"
    )

    # Ensure the join operation for the second GeoPackage is performed and assigned to `joined_data`
    print(
        "Performing join between riverlines and cleaned aggregated data in-memory for the second GeoPackage..."
    )
    joined_data = join_geopackage_tables_in_memory(
        reference_local_path,
        "rec1_Riverlines_SimplifyLine",
        cleaned_data,
        "Top_reach",
        "rchid",
        join_type="inner",
    )
    print("Join operation completed successfully for the second GeoPackage.")

    # Reduce precision for numeric columns before writing second GeoPackage
    cols_to_round_2 = [
        col
        for col in joined_data.columns
        if col.startswith("absolutevalues") or col.startswith("relativevalues")
    ]
    joined_data[cols_to_round_2] = joined_data[cols_to_round_2].round(2)

    # Step 12: Create a new GeoPackage for the second output
    print("Creating a new GeoPackage for the second output...")
    with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as temp_file:
        second_geopackage_path = (
            temp_file.name
        )  # Use tempfile for platform-independent path

    second_output_table_name = "joined_max_riverlines_second"
    write_dataframe_to_geopackage(
        joined_data, second_geopackage_path, second_output_table_name, False
    )
    print(f"Second GeoPackage created with table/layer '{second_output_table_name}'.")

    # Step 13: Upload the second GeoPackage and update the second feature layer
    print("Uploading the second GeoPackage and updating the second feature layer...")
    second_feature_layer = Service(SECOND_FEATURE_LAYER_URL)
    truncate_result = second_feature_layer.manager.truncate()
    if truncate_result["success"]:
        print("Second Feature layer truncated successfully.")
    else:
        print("Failed to truncate the second feature layer.")

    # Step 14: Upload the second GeoPackage and update ArcGIS Online
    print("Uploading the second GeoPackage and updating ArcGIS Online...")
    # Call the function to upload the second GeoPackage and update the second feature layer
    # root_folder = gis.content.folders.get()
    # geopackage_item = upload_geopackage_to_arcgis(
    second_geopackage_item = upload_geopackage_to_arcgis(
        gis,
        second_geopackage_path,
        s3_bucket,
        s3_key,
        second_feature_layer,
        overwrite=False,
    )

    print(
        "Second GeoPackage uploaded and ArcGIS Online updating. Item ID: {second_geopackage_item.id}"
    )

    # Consolidate metadata for both GeoPackages into a single file
    metadata = {
        "first_geopackage": {"item_id": geopackage_item.id},
        "second_geopackage": {"item_id": second_geopackage_item.id},
    }

    # Save the consolidated metadata to a single file
    metadata_file = os.path.join(
        tempfile.gettempdir(), f"consolidated_metadata_{uuid.uuid4().hex}.json"
    )
    with open(metadata_file, "w") as f:
        json.dump(metadata, f)

    # Update the S3 metadata key to use the NetCDF file's S3 key combined with a simple file name
    metadata_file_name = "item_metadata.json"
    s3_metadata_key = f"{s3_key}/{metadata_file_name}"
    print(f"Uploading consolidated metadata file to S3: {s3_metadata_key}")
    s3_client.upload_file(metadata_file, s3_bucket, s3_metadata_key)
    print("Consolidated metadata file uploaded to S3 successfully.")

    return {
        "statusCode": 200,
        "body": "Data update and join operation completed successfully for both GeoPackages.",
    }


# # Uncomment the following lines to test the function locally

# if __name__ == "__main__":
#     # Example event for testing
#     event = {
#         "Records": [
#             {
#                 "s3": {
#                     "bucket": {"name": "s3-lambda-stack-prd-input-bucket-prod"},
#                     "object": {"key": "CONVERT_for_webmap_.nc"},
#                     },
#                 "R1_Geopackage": {
#                     "bucket": {"name": "s3-lambda-stack-prd-input-bucket-prod"},
#                     "object": {"key": "R1_Geopackage/R1_gpkg.gpkg"},
#                     }
#             }

#         ]

#     }
#     context = {}
#     lambda_handler(event, context)
