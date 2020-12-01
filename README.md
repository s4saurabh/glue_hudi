# glue_hudi
Use Glue 2.0 for running the scripts.

To use the glue hudi scripts you need to use the Glue Hudi Custom Shaded jars. 

The jars can be downloaded from these links:

https://aws-bigdata-blog.s3.amazonaws.com/artifacts/hudi-on-glue/Dependencies/Jars/hudi-spark.jar

https://aws-bigdata-blog.s3.amazonaws.com/artifacts/hudi-on-glue/Dependencies/Jars/spark-avro_2.11-2.4.4.jar

Upload the jars to your S3 bucket or directly reference the below S3 locations :

s3://aws-bigdata-blog/artifacts/hudi-on-glue/Dependencies/Jars/hudi-spark.jar

s3://aws-bigdata-blog/artifacts/hudi-on-glue/Dependencies/Jars/spark-avro_2.11-2.4.4.jar

In your Glue ETL job, specify Dependent jars path and add the S3 paths of the jars. Use comma to specify multiple jar files.

Under Job parameters specify:

**Key  :** --conf

**Value:** spark.serializer=org.apache.spark.serializer.KryoSerializer

Also make sure Glue Catalog is selected or pass it under job parameters:
--enable-glue-datacatalog

For more details on custom hudi shaded jars refer https://aws.amazon.com/blogs/big-data/creating-a-source-to-lakehouse-data-replication-pipe-using-apache-hudi-aws-glue-aws-dms-and-amazon-redshift/ which has steps to compile and create the custom shaded jars.
