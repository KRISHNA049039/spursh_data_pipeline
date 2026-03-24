"""
Silver Cleanse Job — AWS Glue ETL
Reads raw AR FX / ICO FX data from Bronze, normalises to the canonical
24-column Silver schema, computes surrogate keys + payload hashes,
deduplicates, and quarantines invalid rows.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET",
        "SOURCE_KEY",
        "SILVER_BUCKET",
        "QUARANTINE_BUCKET",
        "IDEMPOTENCY_TABLE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 1. Read source file from Bronze
# ---------------------------------------------------------------------------

def read_source(bucket: str, key: str):
    path = f"s3://{bucket}/{key}"
    if key.endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    return spark.read.option("multiline", True).json(path)


source_df = read_source(args["SOURCE_BUCKET"], args["SOURCE_KEY"])

# ---------------------------------------------------------------------------
# 2. Map to canonical 24-column Silver schema
# ---------------------------------------------------------------------------
# The source file may use different column names; we normalise here.
# Columns that don't exist in the source are filled with defaults so the
# schema contract is always satisfied.

def _col_or_default(df, source_col, default_val, alias):
    """Return the source column if it exists, otherwise a literal default."""
    if source_col in df.columns:
        return F.col(source_col).alias(alias)
    return F.lit(default_val).alias(alias)


canonical_df = source_df.select(
    # --- Pipeline-derived columns ---
    F.lit("AR_FX").cast(StringType()).alias("source_system"),

    # --- Upstream identifiers ---
    F.col("journal_id").cast(StringType()),
    F.coalesce(F.col("journal_line_num") if "journal_line_num" in source_df.columns
               else F.col("line_num") if "line_num" in source_df.columns
               else F.lit(None), F.lit(None)).cast("int").alias("journal_line_num"),
    F.col("ledger_id").cast(StringType()),
    F.coalesce(F.col("effective_date") if "effective_date" in source_df.columns
               else F.col("acct_date") if "acct_date" in source_df.columns
               else F.lit(None), F.lit(None)).cast("date").alias("effective_date"),

    # period_name derived from effective_date
    F.date_format(
        F.coalesce(
            F.col("effective_date") if "effective_date" in source_df.columns
            else F.col("acct_date") if "acct_date" in source_df.columns
            else F.lit(None),
            F.lit(None),
        ).cast("date"),
        "MMM-yyyy",
    ).alias("period_name"),

    F.col("currency_code").cast(StringType()),

    # --- Amount columns (nullable per spec) ---
    F.col("entered_dr").cast(DecimalType(20, 4)) if "entered_dr" in source_df.columns
        else F.lit(None).cast(DecimalType(20, 4)).alias("entered_dr"),
    F.col("entered_cr").cast(DecimalType(20, 4)) if "entered_cr" in source_df.columns
        else F.lit(None).cast(DecimalType(20, 4)).alias("entered_cr"),
    F.col("accounted_dr").cast(DecimalType(20, 4)) if "accounted_dr" in source_df.columns
        else F.lit(None).cast(DecimalType(20, 4)).alias("accounted_dr"),
    F.col("accounted_cr").cast(DecimalType(20, 4)) if "accounted_cr" in source_df.columns
        else F.lit(None).cast(DecimalType(20, 4)).alias("accounted_cr"),

    F.col("fx_rate").cast(DecimalType(18, 8)) if "fx_rate" in source_df.columns
        else F.lit(None).cast(DecimalType(18, 8)).alias("fx_rate"),

    # --- 7 COA segments ---
    F.col("segment1_entity").cast(StringType()) if "segment1_entity" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment1_entity"),
    F.col("segment2_cost_center").cast(StringType()) if "segment2_cost_center" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment2_cost_center"),
    F.col("segment3_account").cast(StringType()) if "segment3_account" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment3_account"),
    F.col("segment4_project").cast(StringType()) if "segment4_project" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment4_project"),
    F.col("segment5_intercompany").cast(StringType()) if "segment5_intercompany" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment5_intercompany"),
    F.col("segment6_product").cast(StringType()) if "segment6_product" in source_df.columns
        else F.lit(None).cast(StringType()).alias("segment6_product"),
    F.col("segment7_spare").cast(StringType()) if "segment7_spare" in source_df.columns
        else F.col("segment7_spare") if "segment7_spare" in source_df.columns
        else F.lit("000").cast(StringType()).alias("segment7_spare"),

    F.col("description").cast(StringType()) if "description" in source_df.columns
        else F.lit("").cast(StringType()).alias("description"),
)

# ---------------------------------------------------------------------------
# 3. Surrogate key  (MD5 of business-identity columns)
# ---------------------------------------------------------------------------
canonical_df = canonical_df.withColumn(
    "surrogate_key",
    F.md5(
        F.concat_ws(
            "||",
            F.col("source_system"),
            F.coalesce(F.col("journal_id").cast("string"), F.lit("")),
            F.coalesce(F.col("journal_line_num").cast("string"), F.lit("")),
            F.coalesce(F.col("effective_date").cast("string"), F.lit("")),
            F.coalesce(F.col("entered_dr").cast("string"), F.lit("0")),
        )
    ),
)

# ---------------------------------------------------------------------------
# 4. Pipeline metadata columns
# ---------------------------------------------------------------------------
batch_id_val = args["SOURCE_KEY"].split("/")[-1].replace(".json", "").replace(".csv", "")
canonical_df = (
    canonical_df
    .withColumn("batch_id", F.lit(batch_id_val).cast(StringType()))
    .withColumn("load_timestamp", F.current_timestamp())
)

# ---------------------------------------------------------------------------
# 5. Payload hash  (SHA-256 of cols 3-22 per spec)
# ---------------------------------------------------------------------------
payload_cols = [
    "journal_id", "journal_line_num", "ledger_id", "effective_date",
    "period_name", "currency_code", "entered_dr", "entered_cr",
    "accounted_dr", "accounted_cr", "fx_rate",
    "segment1_entity", "segment2_cost_center", "segment3_account",
    "segment4_project", "segment5_intercompany", "segment6_product",
    "segment7_spare", "description", "batch_id",
]
canonical_df = canonical_df.withColumn(
    "payload_hash",
    F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in payload_cols]),
        256,
    ),
)

# ---------------------------------------------------------------------------
# 6. Validate mandatory non-null columns  (quarantine failures)
# ---------------------------------------------------------------------------
NON_NULLABLE = [
    "surrogate_key", "source_system", "journal_id", "journal_line_num",
    "ledger_id", "effective_date", "period_name", "currency_code", "fx_rate",
    "segment1_entity", "segment2_cost_center", "segment3_account",
    "segment4_project", "segment5_intercompany", "segment6_product",
    "segment7_spare", "description", "batch_id", "load_timestamp", "payload_hash",
]

null_filter = F.lit(True)
for col_name in NON_NULLABLE:
    null_filter = null_filter & F.col(col_name).isNotNull()

valid_df = canonical_df.filter(null_filter)
invalid_df = canonical_df.filter(~null_filter)

# ---------------------------------------------------------------------------
# 7. Deduplicate on surrogate_key  (idempotency)
# ---------------------------------------------------------------------------
deduped_df = valid_df.dropDuplicates(["surrogate_key"])

# ---------------------------------------------------------------------------
# 8. Write Silver + Quarantine
# ---------------------------------------------------------------------------
silver_prefix = "silver/fx_adjustments"
quarantine_prefix = "quarantine/fx_adjustments"

deduped_df.write.mode("append").parquet(f"s3://{args['SILVER_BUCKET']}/{silver_prefix}")

if invalid_df.head(1):
    invalid_df.write.mode("append").json(
        f"s3://{args['QUARANTINE_BUCKET']}/{quarantine_prefix}"
    )

job.commit()
