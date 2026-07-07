# Databricks Notebook Source
# Magic Command: %md
# # Cricket Live Score Data Cleaning & Structuring ETL Job
# This PySpark script runs as a Databricks Job triggered by the pipeline scheduler.
# It reads the raw matches JSON payload from S3, flattens the nested structures, 
# performs cleaning, and writes the structured outputs as Parquet.

# COMMAND ----------
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, current_date, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------
# 1. Fetch Job Parameters
# When triggered via API, the scheduler passes the 'india_date' argument
dbutils.widgets.text("india_date", "")
india_date = dbutils.widgets.get("india_date")

# If no date parameter is passed, default to current local date
if not india_date:
    import pytz
    tz = pytz.timezone("Asia/Kolkata")
    india_date = datetime.datetime.now(tz).strftime("%Y-%m-%d")

print(f"Executing Databricks Clean Job for Date: {india_date}")

# COMMAND ----------
# 2. Define S3 Paths
S3_BUCKET = "airflowdemo1817"
RAW_S3_PATH = f"s3a://{S3_BUCKET}/cricket/raw/{india_date}/matches.json"
CLEAN_MATCHES_PATH = f"s3a://{S3_BUCKET}/cricket/clean/{india_date}/"
CLEAN_SCORE_PATH = f"s3a://{S3_BUCKET}/cricket/score/{india_date}/"

# COMMAND ----------
# 3. Read Raw JSON Data
try:
    df_raw = spark.read.option("multiline", "true").json(RAW_S3_PATH)
    print("Successfully read raw S3 file.")
except Exception as e:
    print(f"Error reading S3 raw file. Perhaps file does not exist: {e}")
    dbutils.notebook.exit(f"No file found at path: {RAW_S3_PATH}")

# COMMAND ----------
# 4. Check API Response Validity
# Ensure CricAPI payload was successful and contains matches list
if "data" not in df_raw.columns:
    dbutils.notebook.exit("Invalid payload: CricAPI response does not contain matches data.")

# COMMAND ----------
# 5. Explode Matches Array
# The CricAPI response wraps all matches in a top-level array named "data"
df_matches_exploded = df_raw.select(explode(col("data")).alias("match"))

# COMMAND ----------
# 6. Extract and Clean Matches Metadata
df_matches = df_matches_exploded.select(
    col("match.id").cast(StringType()).alias("MATCH_ID"),
    col("match.teams").getItem(0).cast(StringType()).alias("TEAM_1"),
    col("match.teams").getItem(1).cast(StringType()).alias("TEAM_2"),
    col("match.name").cast(StringType()).alias("MATCH_NAME"),
    col("match.matchType").cast(StringType()).alias("MATCHTYPE"),
    col("match.status").cast(StringType()).alias("STATUS"),
    col("match.date").cast(StringType()).alias("MATCH_DATE_STR"),
    col("match.venue").cast(StringType()).alias("VENUE_NAME"),
    col("match.city").cast(StringType()).alias("CITY"),
    col("match.dateTimeGMT").cast(StringType()).alias("DATETIMEGMT_STR"),
    col("match.matchStarted").cast(BooleanType()).alias("MATCHSTARTED"),
    col("match.matchEnded").cast(BooleanType()).alias("MATCHENDED")
)

# Parse date and timestamps safely
df_matches_clean = df_matches \
    .withColumn("MATCH_DATE", col("MATCH_DATE_STR").cast("date")) \
    .withColumn("DATETIMEGMT", col("DATETIMEGMT_STR").cast("timestamp")) \
    .withColumn("FETCH_DATE", col("DATETIMEGMT").cast("date")) \
    .drop("MATCH_DATE_STR", "DATETIMEGMT_STR")

# Remove any empty rows where Match ID is null
df_matches_clean = df_matches_clean.filter(col("MATCH_ID").isNotNull())

# COMMAND ----------
# 7. Extract and Flatten Inning Scores
# CricAPI provides scores inside a list of objects under the "score" field.
# Each object represents an innings (e.g., {"r": 250, "w": 4, "o": 50, "inning": "Team Name"})
df_scores_raw = df_matches_exploded.select(
    col("match.id").cast(StringType()).alias("MATCH_ID"),
    explode(col("match.score")).alias("score_item")
)

df_scores_clean = df_scores_raw.select(
    col("MATCH_ID"),
    col("score_item.inning").cast(StringType()).alias("TEAM_NAME"),
    col("score_item.r").cast(IntegerType()).alias("RUNS"),
    col("score_item.w").cast(IntegerType()).alias("WICKETS"),
    col("score_item.o").cast(DoubleType()).alias("OVERS")
).filter(col("MATCH_ID").isNotNull())

# Add Innings Order sequentially per Match ID
window_spec = Window.partitionBy("MATCH_ID").orderBy("TEAM_NAME") # fallback ordering
df_scores_final = df_scores_clean.withColumn("INNINGS_ORDER", row_number().over(window_spec))

# COMMAND ----------
# 8. Write Results to S3 as Parquet
print(f"Writing clean matches metadata to: {CLEAN_MATCHES_PATH}")
df_matches_clean.write.mode("overwrite").parquet(CLEAN_MATCHES_PATH)

print(f"Writing clean scores data to: {CLEAN_SCORE_PATH}")
df_scores_final.write.mode("overwrite").parquet(CLEAN_SCORE_PATH)

print("ETL Data Cleaning Job completed successfully.")
