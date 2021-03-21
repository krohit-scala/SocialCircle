package com.socialcircle.consumer

import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils
import org.apache.spark.sql.Encoders
import com.socialcircle.dtos.User
import org.apache.spark.sql.functions.{from_json, split, when}
import com.socialcircle.dtos.UserStats
import com.socialcircle.consumer.foreachwriters.RedisForeachWriter
import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils

object ConsoleTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession
    import spark.implicits._
    
    val newUserDf = {
      spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyFileUtils.getPropertyFromFile("kafka.bootstrap.servers"))
      .option("subscribe", "test1234")
      .load
      .selectExpr(
        "key",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "CAST(value AS STRING) AS jsonData"
      )
    }
    
    // Console output writer
    val q1 = {
      newUserDf.writeStream
      .outputMode("update")
      .format("console")
      .start()
    }
    
    // ES writer
    val newUserSchema = Encoders.product[User].schema
    val q2 = {
      newUserDf
      .select(from_json($"jsonData", newUserSchema).as("data"))
      .select("data.*")
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "~/app_logs/console/spark-streaming/new_user_onboarding/")
      .start("test-new-users")
    }
    
    q1.awaitTermination
    q2.awaitTermination
  }
}