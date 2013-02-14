druid-dumbo, the druid batch config generator
=============================================

When you start to use [batch ingestion](https://github.com/metamx/druid/wiki/Batch-ingestion),
you'll quickly notice, you will need to edit the batch config for each run.

Also, it's unreliable and needs lots of fiddling.

dumbo.rb actually checks your hdfs against your s3 and computes what's needed.

The easiest way to use dumbo is via environment variables:

 * DRUID_DATASOURCE - set it to your druid datasource 
 * DRUID_S3_BUCKET - the s3 bucket to look into
 * DRUID_S3_PREFIX - the s3 prefix to observe
 * DRUID_HDFS_FILEPATTERN - optional, defaults to '/events/*/*/*/*/part*'
 * DRUID_S3_HOST - optional, set to 's3-eu-west-1.amazonaws.com' if you use an EU bucket (strongly recommended for EU people)
 * AMAZON_ACCESS_KEY_ID - your s3 key
 * AMAZON_SECRET_ACCESS_KEY - your s3 secret

Start by creating a `importer.template` based on `importer.template.example`.

Once you got that, try:

```
./dumbo.rb
CLASSPATH=`hadoop classpath`:fullly_qualified_filename_of_druid_indexer_selfcontained_jar
java -cp $CLASSPATH com.metamx.druid.indexer.HadoopDruidIndexerMain ./druidimport.conf 
```

Caveats:

Extremly young code, use at your own risk. Also, currently restricted to hourly granularity

Patches welcome!