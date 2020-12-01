import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor

object GlueHudi {
  def main(sysArgs: Array[String]) {
    val sparkContext: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkContext)
    val spark: SparkSession = glueContext.getSparkSession
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    //Where to store your Hudi Table.
    val outputBucket = "emr-hudi-data-output"
    
    //inputDatasetBucket will use Amazons Public Dataset. 
    //If you wish to use the same dataset (smaller with some intential bad data), see README.md for instructions
    //on how to generate it.
    val inputDatasetBucket = "s3://<<bucket-name>>/amazon-reviews/parquet/"
    
    //Specify common DataSourceWriteOptions in the single hudiOptions variable 
    val hudiTableName = "amazon_product_reviews_glue"
    val hudiTableRecordKey = "review_id"
    val hudiTablePath = "s3://" + outputBucket + "/" + hudiTableName
    val hudiTablePartitionColumn = "review_date"
    val hudiTablePrecombineKey = "timestamp"
    
    val sourceData = spark.read.parquet(inputDatasetBucket + "/*")
    sourceData.write.format("org.apache.hudi").option("hoodie.datasource.hive_sync.use_jdbc","false").option("hoodie.datasource.write.partitionpath.field","review_date").option("hoodie.datasource.hive_sync.partition_extractor_class","org.apache.hudi.hive.MultiPartKeysValueExtractor").option("hoodie.datasource.hive_sync.partition_fields", "review_date").option("hoodie.datasource.write.precombine.field", "review_date").option("hoodie.datasource.write.recordkey.field", "review_id").option("hoodie.table.name", hudiTableName).option("hoodie.datasource.write.operation", "bulk_insert").option("hoodie.bulkinsert.shuffle.parallelism", 3).option("hoodie.consistency.check.enabled", "true").option("hoodie.datasource.hive_sync.table",hudiTableName).option("hoodie.datasource.hive_sync.enable","true").mode("Overwrite").save(hudiTablePath)

  
    //sparkSession.read.format("json").load("s3://emr-hudi-data/amazon-reviews/parquet/").write.format("org.apache.hudi").option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id").option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "").option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "seq").option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL).option(HoodieWriteConfig.TABLE_NAME, "d2").mode(SaveMode.Append).save("s3://<your bucket>/Hudi/Glue/job1/job1_output");
    Job.commit()
  }
}
