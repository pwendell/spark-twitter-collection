import java.util.Date

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.streaming._
import twitter4j._

/**
 * Continuously collect statuses from Twitter and save them into HDFS or S3.
 *
 * Tweets are partitioned by date such that you can create a partitioned Hive table over them.
 */
object TwitterCollector {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    // Local directory for stream checkpointing (allows us to restart this stream on failure)
    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp").toString

    // Output directory
    val outputDir = sys.env.getOrElse("OUTPUT_DIR", "/tmp/tweets_out")

    // Size of output batches in seconds
    val outputBatchInterval = sys.env.get("OUTPUT_BATCH_INTERVAL").map(_.toInt).getOrElse(60)

    // Number of output files per batch interval.
    val outputFiles = sys.env.get("OUTPUT_FILES").map(_.toInt).getOrElse(1)

    // Echo settings to the user
    Seq(("CHECKPOINT_DIR" -> checkpointDir),
        ("OUTPUT_DIR" -> outputDir),
        ("OUTPUT_FILES" -> outputFiles),
        ("OUTPUT_BATCH_INTERVAL" -> outputBatchInterval)).foreach {
      case (k, v) => println("%s: %s".format(k, v))
    }

    outputBatchInterval match {
      case 3600 =>
      case 60 =>
      case _ => throw new Exception(
        "Output batch interval can only be 60 or 3600 due to Hive partitioning restrictions.")
    }

    // Configure Twitter credentials using credentials.txt
    TwitterUtils.configureTwitterCredentials() 

    // Create a local StreamingContext
    val ssc = new StreamingContext("local[12]", "Twitter Downloader", Seconds(30))

    // Enable meta-data cleaning in Spark (so this can run forever)
    System.setProperty("spark.cleaner.ttl", (outputBatchInterval * 5).toString)
    System.setProperty("spark.cleaner.delay", (outputBatchInterval * 5).toString)

    val hiveDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0")
    val year = new java.text.SimpleDateFormat("yyyy")
    val month = new java.text.SimpleDateFormat("MM")
    val day = new java.text.SimpleDateFormat("dd")
    val hour = new java.text.SimpleDateFormat("HH")
    val minute = new java.text.SimpleDateFormat("mm")
    val second = new java.text.SimpleDateFormat("ss")

    // A list of fields we want along with Hive column names and data types
    val fields: Seq[(Status => Any, String, String)] = Seq(
      (s => s.getId, "id", "BIGINT"),
      (s => s.getInReplyToStatusId, "reply_status_id", "BIGINT"),
      (s => s.getInReplyToUserId, "reply_user_id", "BIGINT"),
      (s => s.getRetweetCount, "retweet_count", "INT"),
      (s => s.getText, "text", "STRING"),
      (s => Option(s.getGeoLocation).map(_.getLatitude()).getOrElse(""), "latitude", "FLOAT"),
      (s => Option(s.getGeoLocation).map(_.getLongitude()).getOrElse(""), "longitude", "FLOAT"),
      (s => s.getSource, "source", "STRING"),
      (s => s.getUser.getId, "user_id", "INT"),
      (s => s.getUser.getName, "user_name", "STRING"),
      (s => s.getUser.getScreenName, "user_screen_name", "STRING"),
      (s => hiveDateFormat.format(s.getUser.getCreatedAt), "user_created_at", "TIMESTAMP"),
      (s => s.getUser.getFollowersCount, "user_followers", "BIGINT"),
      (s => s.getUser.getFavouritesCount, "user_favorites", "BIGINT"),
      (s => s.getUser.getLang, "user_language", "STRING"),
      (s => s.getUser.getLocation, "user_location", "STRING"),
      (s => s.getUser.getTimeZone, "user_timezone", "STRING"),

      // Break out date fields for partitioning
      (s => hiveDateFormat.format(s.getCreatedAt), "created_at", "TIMESTAMP"),
      (s => year.format(s.getCreatedAt), "created_at_year", "INT"),
      (s => month.format(s.getCreatedAt), "created_at_month", "INT"),
      (s => day.format(s.getCreatedAt), "created_at_day", "INT"),
      (s => hour.format(s.getCreatedAt), "created_at_hour", "INT"),
      (s => minute.format(s.getCreatedAt), "created_at_minute", "INT"),
      (s => second.format(s.getCreatedAt), "created_at_second", "INT")
    )

    // For making a table later, print out the schema
    val tableSchema = fields.map{case (f, name, hiveType) => "%s %s".format(name, hiveType)}.mkString("(", ", ", ")")
    println("Beginning collection. Table schema for Hive is: %s".format(tableSchema))

    // Remove special characters inside of statuses that screw up Hive's scanner.
    def formatStatus(s: Status): String = {
      def safeValue(a: Any) = Option(a)
        .map(_.toString)
        .map(_.replace("\t", ""))
        .map(_.replace("\"", ""))
        .map(_.replace("\n", ""))
        .map(_.replaceAll("[\\p{C}]","")) // Control characters
        .getOrElse("")

      fields.map{case (f, name, hiveType) => f(s)}
        .map(f => safeValue(f))
        .mkString("\t")
    }

    // Date format for creating Hive partitions
    val outDateFormat = outputBatchInterval match {
      case 60 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH/mm")
      case 3600 => new java.text.SimpleDateFormat("yyyy/MM/dd/HH")
    }

    /** Spark stream declaration */

    // New Twitter stream
    val statuses = ssc.twitterStream()

    // Format each tweet
    val formattedStatuses = statuses.map(s => formatStatus(s))

    // Group into larger batches
    val batchedStatuses = formattedStatuses.window(Seconds(outputBatchInterval), Seconds(outputBatchInterval))

    // Coalesce each batch into fixed number of files
    val coalesced = batchedStatuses.transform(rdd => rdd.coalesce(outputFiles))

    // Save as output in correct directory
    coalesced.foreach((rdd, time) =>  {
       val outPartitionFolder = outDateFormat.format(new Date(time.milliseconds))
       rdd.saveAsTextFile("%s/%s".format(outputDir, outPartitionFolder), classOf[DefaultCodec])
    })

    /** Start Spark streaming */
    ssc.checkpoint(checkpointDir)
    ssc.start()
  }
}
