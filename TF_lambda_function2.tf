# Terraform configuration for deploying the T-Step 2 Lambda function

provider "aws" {
  region = var.aws_region
}

resource "aws_lambda_function" "t_step2" {
  function_name = var.lambda_function_name
  filename      = var.lambda_zip_path
  handler       = "lambda_function2.lambda_handler"
  runtime       = "python3.11"
  role          = var.lambda_role_arn
  timeout       = 900 # 15 minutes (adjust as needed)
  memory_size   = 5048
  ephemeral_storage {
    size = 1500
  }

  # Associate with existing Lambda Layer(s)
  layers = var.lambda_layer_arns != null ? var.lambda_layer_arns : (
  var.lambda_layer_arn != null ? [var.lambda_layer_arn] : null)

  environment {
    variables = {
      OUTPUT_S3_BUCKET         = var.output_s3_bucket
      OUTPUT_S3_KEY            = var.output_s3_key
      FINAL_OUTPUT_KEY         = var.final_output_key
      RIVERLINES_LAYER         = var.riverlines_layer
      MODEL_TABLE              = var.model_table
      LOOKUP_TABLE             = var.lookup_table
      AGOURL                   = var.agourl
      AGOUSERNAME              = var.agousername
      AGOPASSWORD              = var.agopassword
      HOSTED_FEATURE_LAYER_URL = var.hosted_feature_layer_url
      TEMP_ITEM_ID_S3_KEY      = var.temp_item_id_s3_key
    }
  }
  source_code_hash = filebase64sha256(var.lambda_zip_path)
}

# Variables for configuration
variable "aws_region" {}
variable "lambda_function_name" {}
variable "lambda_zip_path" {}
variable "lambda_role_arn" {}
variable "output_s3_bucket" {}
variable "output_s3_key" {}
variable "final_output_key" {}
variable "riverlines_layer" {}
variable "model_table" {}
variable "lookup_table" {}
variable "agourl" {}
variable "agousername" {}
variable "agopassword" {}
variable "hosted_feature_layer_url" {}
variable "temp_item_id_s3_key" { default = "geopackages/last_temp_item_id.txt" }
variable "lambda_layer_arn" { default = null }
variable "lambda_layer_arns" {
  type    = list(string)
  default = null
}

# Example usage:
# terraform apply -var 'aws_region=ap-southeast-2' \
#   -var 'lambda_function_name=t-step2' \
#   -var 'lambda_zip_path=./lambda_function2.zip' \
#   -var 'lambda_role_arn=arn:aws:iam::AWSACCOUNT:role/s3-lambda-stack-prd-LambdaExecutionRole-XXXXXXXXX' \
#   -var 'output_s3_bucket=s3-lambda-stack-prd-output-bucket-prod' \
#   -var 'output_s3_key=your-input-geopackage.gpkg' \
#   -var 'final_output_key=geopackages/final_output.gpkg' \
#   -var 'riverlines_layer=riverlines' \
#   -var 'model_table=data' \
#   -var 'lookup_table=lookup' \
#   -var 'agourl=https://ORGANISATION.maps.arcgis.com' \
#   -var 'agousername=USERNAME' \
#   -var 'agopassword=/AGOPASSWORD' \
#   -var 'hosted_feature_layer_url=https://services3.arcgis.com/fp1tibNcN9mbExhG/arcgis/rest/services/final_output/FeatureServer/0'
#   -var 'lambda_layer_arn=arn:aws:lambda:ap-southeast-2:851725470721:layer:arcgis-sqlite-lambda-stack-prd:2'

# terraform plan -var 'aws_region=ap-southeast-2' -var 'lambda_function_name=step2_TEST' -var 'lambda_zip_path=./lambda_function2.zip' -var 'lambda_role_arn=arn:aws:iam::AWSACCOUNT:role/s3-lambda-stack-prd-LambdaExecutionRole-XXXXXXXXXXX' -var 'output_s3_bucket=s3-lambda-stack-prd-output-bucket-prod' -var 'output_s3_key=your-input-geopackage.gpkg' -var 'final_output_key=geopackages/final_output.gpkg' -var 'riverlines_layer=riverlines' -var 'model_table=data' -var 'lookup_table=lookup' -var 'agourl=https://ORGANISATION.maps.arcgis.com' -var 'agousername=USERNAME' -var 'agopassword=/AGOPASSWORD'   -var 'hosted_feature_layer_url=https://services3.arcgis.com/THEIRACCOUNT/arcgis/rest/services/final_output/FeatureServer/0' -var 'lambda_layer_arn=arn:aws:lambda:ap-southeast-2:MYACCOUNT:layer:arcgis-sqlite-lambda-stack-prd:2'
