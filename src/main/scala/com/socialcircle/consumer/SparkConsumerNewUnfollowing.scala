package com.socialcircle.consumer

import com.socialcircle.consumer.foreachwriters.RedisForeachWriter
import com.socialcircle.utils.PropertyFileUtils

import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils
import org.apache.spark.sql.Encoders
import com.socialcircle.dtos.User
import org.apache.spark.sql.functions.{from_json, split, when, count, coalesce}
import com.socialcircle.dtos.UserStats
import com.socialcircle.consumer.foreachwriters.RedisForeachWriter
import com.socialcircle.consumer.foreachwriters.com.socialcircle.consumer.foreachwriters.NewUnfollowingNeo4JForeachWriter

object SparkConsumerNewUnfollowing {
  
  // REDIS CONNECTOR - FOREACHWRITER SINK
  val redisHost = PropertyFileUtils.getPropertyFromFile("redis.server.ip")
  val redisPort = PropertyFileUtils.getPropertyFromFile("redis.server.port")
  val userStatsHash = PropertyFileUtils.getPropertyFromFile("user.stats.hash")
  
  val redisUserStatsWriter : RedisForeachWriter = new RedisForeachWriter(redisHost, redisPort, userStatsHash)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession
    import spark.implicits._
    
    // 1. Read user following data from Kafka
    val newFollowingDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyFileUtils.getPropertyFromFile("kafka.bootstrap.servers"))
      .option("subscribe", PropertyFileUtils.getPropertyFromFile("kafka.follow.topic"))
      .option("includeHeaders", "true")
      .load
      .selectExpr("CAST(value AS STRING).as(data)")
      .withColumn(
        "user1", 
        split($"data", "\\|")(0)
      ).withColumn(
        "user2", 
        split($"data", "\\|")(1)
      ).withColumn(
        "create_time", 
        split($"data", "\\|")(2)
      )
  
    /*
    // Input Stream
    +-----+-----+---+
    |user1|user2|ts |
    +-----+-----+---+
    | 1001| 1002|ts1|
    | 1002| 1001|ts2|
    | 1003| 1004|ts3|
		+-----+-----+---+
		*/
    
    // 2. Get aggregated followers and followings from Kafka Stream
    val followingDf = newFollowingDf.groupBy("user1").agg(count("*").alias("followings1")).withColumnRenamed("user1", "uId1")
    val followerDf = newFollowingDf.groupBy("user2").agg(count("*").alias("followers1")).withColumnRenamed("user2", "uId2")
    val streamingSummaryDf = followingDf.join(
      followerDf,
      $"uId1" === $"uId2",
      "full"
    ).withColumn(
      "uId",
      coalesce($"uId1", $"uId2")
    ).select(
      "uId", "followers1", "followings1"
    ).na.fill(0, Seq("followings1", "followers1"))

    /*
    // Summary Output
    +------+---------+------------+
    |uId   |followers1|followings1|
    +------+----------+-----------+
    | 1001 |         1|          1|
    | 1002 |         1|          1|
    | 1003 |         0|          1|
    | 1004 |         1|          0|
    +------+----------+-----------+
    */
    
    
    // Read user followers and followings
    // Followers: Number of people following you
    // Followings: Number of people you follow
    val userStatsSchema = Encoders.product[UserStats].schema
    
    // 3. How many followers a given user has?
    val userStatsDf = spark.read
      .format("org.apache.spark.sql.redis")
      .schema(userStatsSchema)
      .option("keys.pattern", s"${userStatsHash}:*") 
      .load
      .selectExpr(
          "userId", 
          "CAST(followers AS INTEGER) AS followers",
          "CAST(followings AS INTEGER) AS followings"
      ).na.fill(0, Seq("followings", "followers"))
    
    // 4. Merge streaming summary with the existing summary
    // For updating the followers/followings
    val joinCondition = ($"uId" === $"userId")
    val colList = Seq("followers", "followings", "followers1", "followings1") // List of columns for null handling
    
    val newSummaryDf = streamingSummaryDf.joinWith(
      userStatsDf,
      joinCondition,
      "full"
    ).withColumn(                                                                    // Fill the missing userId after join
      "uId",
      coalesce($"uId", $"userId").alias("userId")
    ).na.fill(
      0, colList
    ).withColumn(                                                                    // Compute the new followers
      "new_followers",
      ($"followers" - $"followers1")
    ).withColumn(                                                                    // Compute the new followings
      "new_followings",
      ($"followings" - $"followings1")
    ).withColumn(                                                                    // Compute correction for any false incoming record
      "new_followers",
      when($"followers" < 0, 0).otherwise($"followers")
    ).withColumn(                                                                    // Compute correction for any false incoming record
      "new_followings",
      when($"followings" < 0, 0).otherwise($"followings")
    ).where(                                                                         // Identify the updates/new user connections
      ($"new_followers" =!= $"followers") || ($"new_followings" =!= $"followings")
    ).select(                                                                        // Select updates to be pushed to Redis Hash
      $"userId",
      $"new_followings".alias("followings"),
      $"new_followers".alias("followers")
    )
    
    // 5.1. Console output writer (for logging)
    val q1 = {
      newSummaryDf.writeStream
      .outputMode("update")
      .format("console")
      .start()
    }
    
    // 5.2. Update Redis Hash
    val q2 = newSummaryDf.writeStream
      .outputMode("update")
      .foreach(redisUserStatsWriter)
      .start

    // 5.3. Push streaming data to Elasticsearch for archival
    // Checkpoint location 
    val sparkCheckpointLocation = "/home/kr_stevejobs/log_dir/app_logs/spark-streaming_logs/es_writer_new_following/"

    val q3 = {
      newFollowingDf
        .writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("checkpointLocation", "/home/kr_stevejobs/log_dir/app_logs/console/spark-streaming/new_user_unfollowing/")
        .start("new-users-unfollowing")
    }

    // 5.4. Add following relationship to Neo4J
    
    // Neo4j Writer
    // Instantiate the NewUserNeo4JForeachWriter
    val neo4JForeachWriter = new NewUnfollowingNeo4JForeachWriter
    val q4 = {
      newFollowingDf
        .writeStream
        .outputMode("update")
        .foreach(neo4JForeachWriter)
        .start
    }

    // Start Streaming :)
    q1.awaitTermination
    q2.awaitTermination
    q3.awaitTermination
    q4.awaitTermination
    // User2's Followings decreased, thanks to User1
  }
}