from diagrams import Diagram, Cluster
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.integration import SQS
from diagrams.aws.database import Dynamodb
from diagrams.aws.management import Cloudwatch, SSM
from diagrams.aws.security import IAM
from diagrams.aws.analytics import Glue

with Diagram(
    "AWS Lambda NetCDF to ArcGIS Architecture",
    show=True,
    filename="aws_lambda_netcdf_arcgis_architecture",
    outformat="png",
    # directory="."
):
    s3_input = S3("Input S3 Bucket")
    s3_ref = S3("Reference GPKG S3 Bucket")
    sqs = SQS("S3 Event Trigger")
    lambda_fn = Lambda("lambda_function")
    dynamodb = Dynamodb("Metadata Table")
    cloudwatch = Cloudwatch("Logs")
    ssm = SSM("SSM Parameter Store")
    glue = Glue("Data Processing")
    iam = IAM("IAM Role")
    s3_output = S3("Output S3 Bucket")

    s3_input >> sqs >> lambda_fn
    s3_ref >> lambda_fn
    ssm >> lambda_fn
    lambda_fn >> glue
    lambda_fn >> dynamodb
    lambda_fn >> cloudwatch
    lambda_fn >> s3_output
    iam >> lambda_fn