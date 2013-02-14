druid-dumbo, the druid batch config generator
=============================================

When you start to use [batch ingestion](https://github.com/metamx/druid/wiki/Batch-ingestion),
you'll quickly notice, you will need to edit the batch config for each run.

druid-dumbo actually checks your HDFS against your S3 and computes what's missing/outdated.

The easiest way to use dumbo is via environment variables:

 * DRUID_DATASOURCE - set it to your druid datasource 
 * DRUID_S3_BUCKET - the s3 bucket to look into
 * DRUID_S3_PREFIX - the s3 prefix to observe
 * DRUID_HDFS_FILEPATTERN - optional, defaults to '/events/*/*/*/*/part*'
 * DRUID_S3_HOST - optional, set to 's3-eu-west-1.amazonaws.com' if you use an EU bucket (strongly recommended for EU people)
 * AMAZON_ACCESS_KEY_ID - your s3 key
 * AMAZON_SECRET_ACCESS_KEY - your s3 secret

Start by creating an `importer.template` based on `importer.template.example`.

Once you got that, try:

```
DRUIDBASE=fully_qualified_path_to_druid # PLEASE ADJUST
CLASSPATH=`hadoop classpath`:`find $DRUIDBASE/indexer/target/ -name druid-indexer-*-selfcontained.jar`

./dumbo.rb
java -cp $CLASSPATH com.metamx.druid.indexer.HadoopDruidIndexerMain ./druidimport.conf 
```

Caveats
-------

Extremly young code, use at your own risk. Also, currently restricted to hourly granularity and JSON in HDFS.

You can support us on different ways
------------------------------------

* Use druid-dumbo, and let us know if you encounter anything that's broken or missing.
  A failing spec is great. A pull request with your fix is even better!
* Spread the word about druid-dumbo on Twitter, Facebook, and elsewhere.
* Work with us at madvertise on awesome stuff like this.
  [Read the job description](http://madvertise.com/en/2013/02/07/software-developer-ruby-fm) and send a mail to careers@madvertise.com.

