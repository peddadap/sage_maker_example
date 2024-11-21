import os
import shutil
import argparse
import boto3
import sagemaker
import logging
logging.basicConfig(level=logging.DEBUG)
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
parser = argparse.ArgumentParser(description="Submit a SageMaker PySparkProcessor job.")
parser.add_argument("--role", required=True, help="IAM role ARN for SageMaker to use.")
parser.add_argument("--input_s3_path", required=True, help="S3 path for input data.")
parser.add_argument("--output_s3_path", required=True, help="S3 path for job output.")
parser.add_argument("--bucket", required=True, help="S3 bucket to upload the zip file.")
parser.add_argument("--zip_name", required=True, help="Name of the zip file (without extension).")
args = parser.parse_args()

# Dynamically determine the source directory
current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script's directory
source_dir = os.path.abspath(os.path.join(current_dir, "../"))  # Navigate to `my_app`

# Check if the source directory exists
if not os.path.exists(source_dir):
    raise FileNotFoundError(f"Source directory {source_dir} does not exist!")


# Dynamically create the zip file
create_zip(args.zip_name, source_dir)

# Upload the zip file to S3 (using the default profile)
s3 = boto3.client("s3")
s3_zip_path = f"s3://{args.bucket}/inputs/{args.zip_name}.zip"
s3.upload_file(f"{args.zip_name}.zip", args.bucket, f"inputs/{args.zip_name}.zip")
print(f"Uploaded {args.zip_name}.zip to {s3_zip_path}")

# Initialize SageMaker session
sagemaker_session = sagemaker.Session()

# Create PySparkProcessor
spark_processor = PySparkProcessor(
    framework_version="3.1",
    role=args.role,
    instance_type="ml.t3.medium",
    instance_count=1,
    base_job_name="spark-app-zip-job",
    sagemaker_session=sagemaker_session
)

# Submit the PySparkProcessor job
spark_processor.run(
    submit_app=f"{s3_zip_path}!job_script.py",  # Reference the main script inside the zip
    logs=True,  # Enable CloudWatch logging
    inputs=[
        ProcessingInput(
            source=args.input_s3_path,  # Input data location in S3
            destination="/opt/ml/processing/input"  # Where input is mounted in the container
        )
    ],
    outputs=[
        ProcessingOutput(
            source="/opt/ml/processing/output",  # Output location in container
            destination=args.output_s3_path  # Output S3 path
        )
    ]
)

print("Processing job submitted successfully!")
