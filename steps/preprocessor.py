import os
import shutil
import argparse
import boto3
import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput

# Function to dynamically create a zip file
def create_zip(zip_name, source_dir):
    """
    Create a zip file of the source directory.
    :param zip_name: Name of the zip file to be created
    :param source_dir: Directory containing files to be zipped
    """
    shutil.make_archive(zip_name, 'zip', source_dir)
    print(f"Created zip archive: {zip_name}.zip")

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Submit a SageMaker PySparkProcessor job with dynamic zipping.")
parser.add_argument("--role", required=True, help="IAM role ARN for SageMaker to use.")
parser.add_argument("--input_s3_path", required=True, help="S3 path for input data.")
parser.add_argument("--output_s3_path", required=True, help="S3 path for job output.")
parser.add_argument("--bucket", required=True, help="S3 bucket to upload the zip file.")
parser.add_argument("--source_dir", required=True, help="Directory containing job scripts and dependencies.")
parser.add_argument("--zip_name", required=True, help="Name of the zip file (without extension).")
args = parser.parse_args()

# Dynamically create the zip file
create_zip(args.zip_name, args.source_dir)

# Upload the zip file to S3
s3 = boto3.client("s3")
s3_zip_path = f"s3://{args.bucket}/path-to-zip/{args.zip_name}.zip"
s3.upload_file(f"{args.zip_name}.zip", args.bucket, f"path-to-zip/{args.zip_name}.zip")
print(f"Uploaded {args.zip_name}.zip to {s3_zip_path}")

# Initialize SageMaker session
sagemaker_session = sagemaker.Session()

# Create PySparkProcessor
spark_processor = PySparkProcessor(
    framework_version="3.1",
    role=args.role,
    instance_type="ml.m5.xlarge",
    instance_count=1,
    base_job_name="dynamic-zip-spark-job",
    sagemaker_session=sagemaker_session
)

# Submit the PySparkProcessor job
spark_processor.run(
    submit_app="main_script.py",  # Entry point script in the zip
    submit_dependencies=[s3_zip_path],  # Dependencies zip file
    inputs=[
        ProcessingInput(
            source=args.input_s3_path,  # Input data location in S3
            destination="/opt/ml/processing/input"  # Where input is mounted in the container
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/output",  # Where output is written
            destination=args.output_s3_path  # Output data location in S3
        )
    ]
)

print("Processing job submitted successfully!")
