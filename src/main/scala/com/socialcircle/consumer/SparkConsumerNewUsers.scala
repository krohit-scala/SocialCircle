package com.socialcircle.consumer

import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils
import org.apache.spark.sql.Encoders
import com.socialcircle.dtos.User
import org.apache.spark.sql.functions.{from_json, split, when}
import com.socialcircle.dtos.UserCount
import com.socialcircle.consumer.foreachwriters.RedisForeachWriter

object SparkConsumerNewUsers {
  def main(args: Array[String]): Unit = {
  
    // Get SparkSession object
    val spark = SparkUtils.getSparkSession
    import spark.implicits._
    
    // 1. Read user onboarding data from Kafka
    val newUserSchema = Encoders.product[User].schema
    
    val newUserDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyFileUtils.getPropertyFromFile("bootstrap.servers"))
      .option("subscribe", PropertyFileUtils.getPropertyFromFile("kafka.new.user.topic"))
      .option("includeHeaders", "true")
      .load
      .selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", newUserSchema).as("data"))
      .select("data.*")
      
    // Write the new user data to Elasticsearch sink
    val q1 = newUserDf.writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "~/app_logs/spark-streaming/new_user_onboarding/")
      .start("social-circle-users/doc-type")
    
    // Start Streaming :-)
    q1.awaitTermination
  }
}