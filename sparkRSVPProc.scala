import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, concat, from_json, lit, when, window}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

object big230_1 {
  def main(args: Array[String]): Unit = {
    // Create a SparkConfig Object and SparkContext to initialize Spark


    val ss = SparkSession
      .builder()
      .appName("Final Application")
      .master("local[*]")
      .getOrCreate()

    // This import is needed
    import ss.implicits._


    //Prepare schema
    val jsonSchema = new StructType()
      .add("event",
        new StructType()
          .add("event_id", StringType)
          .add("event_name", StringType)
          .add("event_url", StringType)
          .add("time", LongType) )

      .add("group",
        new StructType()
          .add("group_city", StringType)
          .add("group_country", StringType)
          .add("group_id", StringType)
          .add("group_lat", DoubleType)
          .add("group_lon", DoubleType)
          .add("group_name", StringType)
          .add("group_state", StringType)
          .add("group_topics",
            ArrayType(
              new StructType()
                .add("topic_name", StringType)
                .add("urlkey", StringType)
            ))
          .add("group_urlname", StringType))

      .add("guests", IntegerType)

      .add("member",
        new StructType()
          .add("member_id", StringType)
          .add("member_name", StringType)
          .add("other_services",
            new StructType()
              .add("facebook",
                new StructType()
                  .add("identifier", StringType))
              .add("linkedin",
                new StructType()
                  .add("identifier", StringType))
              .add("twitter",
                new StructType()
                  .add("identifier", StringType))
          )
          .add("photo", StringType)
      )
      .add("mtime", TimestampType)
      .add("response", StringType)
      .add("rsvp_id", LongType)
      .add("venue",
        new StructType()
          .add("lat", DoubleType)
          .add("lon", DoubleType)
          .add("venue_id", LongType)
          .add("venue_name", StringType))
      .add("visibility", StringType)


    // Start the streaming structured df

    val ssdf_unparsed0 = ss.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","rsvp01").option("failOnDataLoss", "false").load()


    val ssdf_unparsed = ssdf_unparsed0.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")



    //Parse based on the schema
    val ssdf = ssdf_unparsed.select(from_json( $"value", jsonSchema) as "parsed", $"timestamp")
      .select($"parsed.*",$"timestamp")
      .select($"timestamp", $"rsvp_id",$"member.*", $"response", $"mtime", $"venue.*", $"guests", $"group.*")
      .withColumn("venue_coordinate", array($"lon",$"lat"))
      .withColumn("group_coordinate", array($"group_lon",$"group_lat"))


    val ssdf_tokafka = ssdf.selectExpr("CAST(rsvp_id AS STRING) AS key", "to_json(struct(*)) AS value")



/*
    //Calculate by country Y response rate
    val ssdf_stat = ssdf
      .withWatermark("timestamp", "10 minutes")
      .withColumn("response_int", when($"response" === "no", 0).otherwise(1))
      . groupBy(window($"timestamp", "1 minute"),$"group_country").agg( avg($"response_int").as("response_rate"))

    // Structure as key and value and cast as string
    val ssdf_tokafka = ssdf_stat.selectExpr("CAST(window AS STRING)", "group_country", "CAST(response_rate AS STRING)")
      .select(concat($"window", lit(" "), $"group_country").as("key"),$"response_rate".as("value"))

    //ssdf_tokafka.printSchema()
*/
    // Write into kafka stream
    val kafkaOutput = ssdf_tokafka.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rsvp01Processed")
      .option("checkpointLocation", "/home/afshin/Documents/kafka_checkpoint/rsvp01Processed")
      .option("failOnDataLoss", "false")
      .start()

    kafkaOutput.awaitTermination()




    // Print to console
    val query0 = ssdf_tokafka.writeStream.outputMode("complete").format("console").start()


    // Run the Code until stopped
    query0.awaitTermination()


    // Here we could have other analyses

  }
}
