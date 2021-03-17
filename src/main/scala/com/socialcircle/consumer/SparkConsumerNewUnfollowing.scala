package com.socialcircle.consumer

import com.socialcircle.consumer.foreachwriters.RedisForeachWriter
import com.socialcircle.utils.PropertyFileUtils

import com.socialcircle.utils.SparkUtils
import com.socialcircle.utils.PropertyFileUtils
import org.apache.spark.sql.Encoders
import com.socialcircle.dtos.User
import org.apache.spark.sql.functions.{from_json, split, when}
import com.socialcircle.dtos.UserCount
import com.socialcircle.consumer.foreachwriters.RedisForeachWriter

object SparkConsumerNewUnfollowing {
  
  // REDIS CONNECTOR - FOREACHWRITER SINK
  val redisHost = PropertyFileUtils.getPropertyFromFile("redis.server.ip")
  val redisPort = PropertyFileUtils.getPropertyFromFile("redis.server.port")
  val userStatsHash = PropertyFileUtils.getPropertyFromFile("user.following.stats.hash")
  
  val redisUserStatsWriter : RedisForeachWriter = new RedisForeachWriter(redisHost, redisPort, userStatsHash)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession
    import spark.implicits._
    
    // 2. Read user following data from Kafka
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
    
    // Read user followers and followings
    // Followers: Number of people following you
    // Followings: Number of people you follow
    val userCountsSchema = Encoders.product[UserCount].schema
    
    // How many followers a given user has?
    val userStatsDf = spark.read
      .format("org.apache.spark.sql.redis")
      .schema(userCountsSchema)
      .option("keys.pattern", "followerCount:*") 
      .load
      .selectExpr(
          "userId", 
          "CAST(counts AS INTEGER) AS followers",
          "CAST(counts AS INTEGER) AS followings"
      )
      
    // For updating the followers/followings
    val joinCondition1 = ($"user1" === $"userId")
    val joinCondition2 = ($"user2" === $"userId")

    // Join Streaming dataframe with followers/followings dataframe form Redis
    // User1|User2 => User1 started following User2 | User1's following -= 1
    val joinedDf1 = newFollowingDf.joinWith(
      userStatsDf,
      joinCondition1,
      "left"
    ).select(
      "_1.*",
      "_2.*"
    ).withColumn(
      "followings", 
      when($"followings" === 0, 1).otherwise($"followings" - 1)
    ).joinWith(
      userStatsDf,
      joinCondition2,
      "left"
    ).select(
      "_1.*",
      "_2.*"
    ).withColumn(
      "followings", 
      when($"followers" === 0, 1).otherwise($"followers" - 1)
    )
    
//    val joinedDf2 = newFollowingDf.joinWith(
//      userStatsDf,
//      joinCondition2,
//      "left"
//    ).select(
//      "_1.*",
//      "_2.*"
//    ).withColumn(
//      "followings", 
//      when($"followers" === 0, 1).otherwise($"followers" - 1)
//    )
    
    val userStatsUpdateDf1 = joinedDf1.selectExpr(
      "userId", 
      "CAST(followers AS STRING) AS followers",
      "CAST(followings AS STRING) AS followings"
    )

//    val userStatsUpdateDf2 = joinedDf2.selectExpr(
//      "userId", 
//      "CAST(followers AS STRING) AS followers",
//      "CAST(followings AS STRING) AS followings"
//    )

    val q1 = userStatsUpdateDf1.writeStream
      .outputMode("update")
      .foreach(redisUserStatsWriter)
      .start

//    val q2 = userStatsUpdateDf2.writeStream
//      .outputMode("update")
//      .foreach(redisUserStatsWriter)
//      .start
    
    q1.awaitTermination
//    q2.awaitTermination
    // User2's Followings decreased, thanks to User1
  }
}