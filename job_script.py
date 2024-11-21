# main_script.py
from pyspark.sql import SparkSession
from scripts.text_module import LARGE_TEXT  # Import the large text from the subdirectory

# Create Spark session
spark = SparkSession.builder.appName("TextProcessingJob").getOrCreate()

# Split the text into words
words = LARGE_TEXT.split()
word_count = len(words)

# Print results to the console (captured in logs)
print(f"Word count: {word_count}")

# Save results to the output directory
output_path = "/opt/ml/processing/output/result.txt"
with open(output_path, "w") as f:
    f.write(f"Word count: {word_count}")

print(f"Results saved to {output_path}")
