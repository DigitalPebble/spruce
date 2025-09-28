# Use the AWS CLI to generate a data export for CURv2 reports

read -p "Enter Region [eu-north-1]: " region
region=${region:-eu-north-1}

read -p "Enter bucket name: " bucket

read -p  "Enter prefix: " prefix

echo "Generating dataexport-definition.json"
cat <<EOT > dataexport-definition.json
{
    "DataQuery": {
        "QueryStatement": "SELECT bill_bill_type, bill_billing_entity, bill_billing_period_end_date, bill_billing_period_start_date, bill_invoice_id, bill_invoicing_entity, bill_payer_account_id, bill_payer_account_name, cost_category, discount, discount_bundled_discount, discount_total_discount, identity_line_item_id, identity_time_interval, line_item_availability_zone, line_item_blended_cost, line_item_blended_rate, line_item_currency_code, line_item_legal_entity, line_item_line_item_description, line_item_line_item_type, line_item_net_unblended_cost, line_item_net_unblended_rate, line_item_normalization_factor, line_item_normalized_usage_amount, line_item_operation, line_item_product_code, line_item_resource_id, line_item_tax_type, line_item_unblended_cost, line_item_unblended_rate, line_item_usage_account_id, line_item_usage_account_name, line_item_usage_amount, line_item_usage_end_date, line_item_usage_start_date, line_item_usage_type, pricing_currency, pricing_lease_contract_length, pricing_offering_class, pricing_public_on_demand_cost, pricing_public_on_demand_rate, pricing_purchase_option, pricing_rate_code, pricing_rate_id, pricing_term, pricing_unit, product, product_comment, product_fee_code, product_fee_description, product_from_location, product_from_location_type, product_from_region_code, product_instance_family, product_instance_type, product_instancesku, product_location, product_location_type, product_operation, product_pricing_unit, product_product_family, product_region_code, product_servicecode, product_sku, product_to_location, product_to_location_type, product_to_region_code, product_usagetype, resource_tags, savings_plan_net_amortized_upfront_commitment_for_billing_period, savings_plan_net_recurring_commitment_for_billing_period, savings_plan_net_savings_plan_effective_cost, savings_plan_offering_type, savings_plan_payment_option, savings_plan_purchase_term, savings_plan_recurring_commitment_for_billing_period, savings_plan_region, savings_plan_savings_plan_a_r_n, savings_plan_savings_plan_effective_cost, savings_plan_savings_plan_rate, savings_plan_start_time, savings_plan_total_commitment_to_date, savings_plan_used_commitment FROM COST_AND_USAGE_REPORT",
        "TableConfigurations": {
            "COST_AND_USAGE_REPORT": {
                "INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY": "FALSE",
                "INCLUDE_RESOURCES": "TRUE",
                "INCLUDE_SPLIT_COST_ALLOCATION_DATA": "FALSE",
                "TIME_GRANULARITY": "HOURLY"
            }
        }
    },
    "Description": "CURv2 DataExport",
    "DestinationConfigurations": {
        "S3Destination": {
            "S3Bucket": "${bucket}",
            "S3OutputConfigurations": {
                "Compression": "PARQUET",
                "Format": "PARQUET",
                "OutputType": "CUSTOM",
                "Overwrite": "OVERWRITE_REPORT"
            },
            "S3Prefix": "${prefix}",
            "S3Region": "${region}"
        }
    },
    "Name": "CURv2-dataexport",
    "RefreshCadence": {
        "Frequency": "SYNCHRONOUS"
    }
}
EOT

echo "Creating S3 bucket..."
aws s3 mb s3://${bucket} --region ${region}
echo "S3 bucket created successfully"

echo "Generating bucket policy"
cat <<EOT > dataexport-s3-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "EnableAWSDataExportsToWriteToS3AndCheckPolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "billingreports.amazonaws.com",
                    "bcm-data-exports.amazonaws.com"
                ]
            },
            "Action": [
                "s3:PutObject",
                "s3:GetBucketPolicy"
            ],
            "Resource": [
                "arn:aws:s3:::${bucket}",
                "arn:aws:s3:::${bucket}/*"
            ]
        }
    ]
}
EOT


echo "Attaching policy to S3 bucket ${bucket}..."
aws s3api put-bucket-policy --bucket ${bucket} --policy file://dataexport-s3-policy.json
echo "S3 bucket policy added successfully"


echo "Creating the billing data export..."
aws bcm-data-exports create-export --export file://dataexport-definition.json

