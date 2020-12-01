import sys

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import Row
from pyspark.sql.functions import concat, col, lit, to_timestamp

#sys.argv = ['inputVariable1', 'value1','--JOB_NAME', 'test']

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

# CONSTANTS
outputBucket = "hudi-data-output"
inputDatasetBucket = "s3://<<bucket-name>>/amazon-reviews/parquet/"
hudiTableName = "amazon_product_reviews_cow"
hudiTableRecordKey = "review_id"
hudiTablePath = "s3://" + outputBucket + "/" + hudiTableName
hudiTablePartitionColumn = "review_date"
hudiTablePrecombineKey = "review_date"

sourceData = spark.read.parquet(inputDatasetBucket + "/*")

sourceData.write.format("org.apache.hudi").option("hoodie.datasource.hive_sync.use_jdbc","false").option("hoodie.datasource.write.partitionpath.field","review_date").option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor").option("hoodie.datasource.hive_sync.partition_fields", "review_date").option("hoodie.datasource.write.precombine.field", "review_date").option("hoodie.datasource.write.recordkey.field", "review_id").option("hoodie.table.name", hudiTableName).option("hoodie.datasource.write.operation", "bulk_insert").option("hoodie.bulkinsert.shuffle.parallelism", 3).option("hoodie.consistency.check.enabled", "true").option("hoodie.datasource.hive_sync.table",hudiTableName).option("hoodie.datasource.hive_sync.enable","true").mode("Overwrite").save(hudiTablePath)

job.commit()
