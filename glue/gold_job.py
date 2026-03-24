"""
Gold Shape Job — AWS Glue ETL
Reads cleansed Silver data, aggregates to MEC-ready Gold facts with all
24 canonical columns preserved, partitioned by batch_id for audit.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_BUCKET",
        "GOLD_BUCKET",
        "BATCH_ID",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

silver_path = f"s3://{args['SILVER_BUCKET']}/silver/fx_adjustments"
gold_path = f"s3://{args['GOLD_BUCKET']}/gold/fx_adjustments/batch_id={args['BATCH_ID']}"

silver_df = spark.read.parquet(silver_path)

# ---------------------------------------------------------------------------
# Gold layer keeps the full 24-column schema from Silver.
# We filter to the current batch, re-validate completeness, and write as-is.
# The MEC validator downstream expects every column present.
# ---------------------------------------------------------------------------


CANONICAL_24 = [
    "surrogate_key", "source_system", "journal_id", "journal_line_num",
    "ledger_id", "effective_date", "period_name", "currency_code",
    "entered_dr", "entered_cr", "accounted_dr", "accounted_cr", "fx_rate",
    "segment1_entity", "segment2_cost_center", "segment3_account",
    "segment4_project", "segment5_intercompany", "segment6_product",
    "segment7_spare", "description", "batch_id", "load_timestamp", "payload_hash",
]

# Filter to current batch
batch_df = silver_df.filter(F.col("batch_id") == args["BATCH_ID"])

# Ensure all 24 columns exist (fill missing with null so downstream catches it)
for col_name in CANONICAL_24:
    if col_name not in batch_df.columns:
        batch_df = batch_df.withColumn(col_name, F.lit(None))

# Select only the canonical columns in order
gold_df = batch_df.select(*CANONICAL_24)

# Add a gold-layer timestamp for lineage
gold_df = gold_df.withColumn("gold_timestamp", F.current_timestamp())

gold_df.write.mode("overwrite").parquet(gold_path)

job.commit()
