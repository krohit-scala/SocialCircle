package com.socialcircle.userEvents

import com.socialcircle.utils.RedisUtils
import org.apache.spark.streaming.kafka010.KafkaUtils
import com.socialcircle.producer.KafkaProducerApi
import com.socialcircle.utils.PropertyFileUtils
import com.socialcircle.utils.CommonUtils

object FollowingEvents {
  
  // Redis set for all active users
  // Format: UserId
  val activeUserSetRedis = PropertyFileUtils.getPropertyFromFile("active.user.set.redis")

  // Redis set for persisting data when user starts following another user
  // Format: "userId1|userId2" : User1 started following User2
  val userFollowingSetRedis = PropertyFileUtils.getPropertyFromFile("user.following.set.redis")
  
  // Kafka topic to register user followings
  val kafkaFollowTopic = PropertyFileUtils.getPropertyFromFile("kafka.follow.topic")

  // Kafka topic to register user unfollowings
  val kafkaUnfollowTopic = PropertyFileUtils.getPropertyFromFile("kafka.unfollow.topic")


  // Method to make a randomly chosen active user follow another active user
  def simulateUserFollowing : Unit = {
    // Pull two users at random from set of active/onboarded users from Redis
    val randomUserPair = RedisUtils.jedis.srandmember(activeUserSetRedis, 2).toArray
    
    // Add new following data if it didn't previously exists
    if(randomUserPair != null && randomUserPair.size == 2 ){
      val user1 = randomUserPair(0).toString
      val user2 = randomUserPair(1).toString
      val ts = CommonUtils.getTimestampLongFormat
      
      // Data: "User1|User2|ts" => User1 started following User2 at TS
      val data = s"${user1}|${user2}|${ts}"
      
      if( (!RedisUtils.checkItemInSet(userFollowingSetRedis, data)) && (!user1.equals(user2)) ){
        RedisUtils.addToSet(userFollowingSetRedis, data)
        println(s"User1: '${user1}' started following User2: '${user2}' from: ${ts}.")
        
        // Send message to Kafka about following event
        KafkaProducerApi.sendSingleMessageProducer(kafkaFollowTopic, data)
      }
    }
    else{
      println("[ERROR] : Not enough active users to simulate user following...")
    }
  }
  
  // Method to simulate random user unfollow another user
  def simulateUserUnfollowing: Unit = {
    // Pull a pair from userFollows set in Redis and make them unfollow
    // Format: "userId1|userId2|ts" : User1 started following User2 from TS onwards
    // Removing this pair from Redis set will simulate the unfollow action
    // User1 started unfollowing User2
    val randomUserPair = RedisUtils.jedis.srandmember(userFollowingSetRedis, 1).toArray
    
    if(!(randomUserPair == null || randomUserPair.isEmpty)){
      println(s"randomUserPair(0).toString: ${randomUserPair(0).toString}")
      val user1 = randomUserPair(0).toString.split("\\|")(0)
      val user2 = randomUserPair(0).toString.split("\\|")(1)
      val ts = randomUserPair(0).toString.split("\\|")(2)
      val dataRemoved = s"${user1}|${user2}|${ts}"
      
      // If jedis.srem("${user1}|${user2}") == 1, User1 starts unfollowing User2
      val isRemoved = RedisUtils.jedis.srem(userFollowingSetRedis, dataRemoved)
      // println(s"Users identified for removal: ${data}. Value of isRemoved: ${isRemoved}")
      if(isRemoved == 1){
        println(s"User1: '${user1}' started unfollowing User2: '${user2}'")
        
        // Send message to Kafka about unfollowing event
        val unfollowTs = CommonUtils.getTimestampLongFormat
        val dataRemoved = s"${user1}|${user2}|${unfollowTs}"
        KafkaProducerApi.sendSingleMessageProducer(kafkaUnfollowTopic, dataRemoved)
      }
      else{
        println(s"[Error] : Failed to register unfollow request >> User1: '${user1}' unfollowing User2: '${user2}'")
      }
    }
  }
}