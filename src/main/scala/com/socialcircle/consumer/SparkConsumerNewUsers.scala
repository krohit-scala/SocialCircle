package com.socialcircle.consumer

import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils
import org.apache.spark.sql.Encoders
import com.socialcircle.dtos.User
import org.apache.spark.sql.functions.{from_json, split, when}
import com.socialcircle.dtos.UserStats
import com.socialcircle.consumer.foreachwriters.RedisForeachWriter
import com.socialcircle.consumer.foreachwriters.NewUserNeo4JForeachWriter

object SparkConsumerNewUsers {
  def main(args: Array[String]): Unit = {
  
    // Get SparkSession object
    val spark = SparkUtils.getSparkSession
    import spark.implicits._

    // Schema for NewUser Streaming Data
    val newUserSchema = Encoders.product[User].schema
    
    // Instantiate the NewUserNeo4JForeachWriter
    val newUserNeo4JForeachWriter = new NewUserNeo4JForeachWriter
 
    // Checkpoint location 
    val sparkCheckpointLocation = "/home/kr_stevejobs/log_dir/app_logs/spark-streaming_logs/es_writer_new_user_onboarding/"

    // 1. Read user onboarding data from Kafka
    val newUserDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyFileUtils.getPropertyFromFile("kafka.bootstrap.servers"))
      .option("subscribe", PropertyFileUtils.getPropertyFromFile("kafka.new.user.topic"))
      .option("includeHeaders", "true")
      .load
      .selectExpr("CAST(value AS STRING) AS jsonData")
      
    // 2. Write the new user data to Elasticsearch sink
    val q1 = {
      newUserDf
        .select(from_json($"jsonData", newUserSchema).as("data"))    // Convert JSON data to nested column in Spark Dataframe
        .select("data.*")
        .writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("checkpointLocation", sparkCheckpointLocation)
        .start("social-circle-users")
    }
    
    // 3. Write new user data to Neo4j
    val q2 = {
      newUserDf
        .select("jsonData")
        .as[String]
        .writeStream
        .outputMode("update")
        .foreach(newUserNeo4JForeachWriter)
        .start
    }
    
    // Start Streaming :-)
    q1.awaitTermination
    q2.awaitTermination
  }
}