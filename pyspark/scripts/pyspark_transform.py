from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, when, month, current_date, datediff # type: ignore
from pyspark.sql.types import StringType # type: ignore

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

# Read data from GCS
df = spark.read.csv("gs://YOUR_GCP_BUCKET/data.csv", header=True)

# Remove duplicates
df = df.dropDuplicates()

# Check for missing loanee_id and remove rows
df = df.filter(col("loanee_id").isNotNull())

# Add new columns for transformation
df = df.withColumn("PaidOnTime", when(col("date_repaid").isNull(), "N/A")
                   .when(col("date_repaid") <= col("expected_repayment_date"), "Yes").otherwise("No"))

df = df.withColumn("LateTime", when(datediff(col("date_repaid"), col("expected_repayment_date")) <= 7, "< 7")
                  .otherwise("> 7"))

df = df.withColumn("PaidMonth", when(col("date_repaid").isNotNull(), month(col("date_repaid")).cast(StringType())).otherwise(None))

df = df.withColumn("DefaultStatus", when(col("date_repaid").isNull() & (current_date() > col("expected_repayment_date")), "Yes").otherwise("No"))

# Select required columns and save the transformed data back to GCS
selected_columns = ["loanee_id", "name", "age", "gender", "amount_borrowed", "date_borrowed", 
                    "expected_repayment_date", "date_repaid", "amount_to_be_repaid", "employment_status", 
                    "income", "credit_score", "loan_purpose", "loan_type", "interest_rate", "loan_term", 
                    "address", "city", "state", "zip_code", "country", "email", "phone_number", 
                    "marital_status", "dependents", "education_level", "employer", "job_title", 
                    "years_employed", "PaidOnTime", "LateTime", "PaidMonth", "DefaultStatus"]

df_transformed = df.select(selected_columns)

# Write transformed data back to GCS
df_transformed.write.csv("gs://YOUR_GCP_BUCKET/data_transformed.csv", header=True)

# Stop Spark session
spark.stop()
