package com.socialcircle.userEvents

import scala.collection.immutable.HashSet

import com.socialcircle.dtos.User
import com.socialcircle.utils.DBUtils
import com.socialcircle.utils.RedisUtils
import com.socialcircle.utils.PropertyFileUtils

object OnboardNewUser {
  
  // Set of all active users in Redis
  val activeUserSetRedis = PropertyFileUtils.getPropertyFromFile("active.user.set.redis")
  
  // Onboard a randomly chosen new user
  def newUserOnboarding : User = {
    // User Id range from : 100000 to 110000
    val random = new scala.util.Random
    val start = 100001
    val end = 110000
    
    // Flag to check if user is already onboarded
    var userAlreadyExists = true
    
    // Get new user from database
    var newUserId = -1
    while(userAlreadyExists){
      // Generate a random user id for onboarding
      val tempUserId = start + random.nextInt( (end - start) + 1)
      
      // Check if the user is already onboarded (in Redis)
      // println(s"Checking for any existing user with id: ${tempUserId.toString}.")
      if(!RedisUtils.checkItemInSet(activeUserSetRedis, tempUserId.toString)){
        userAlreadyExists = false
        newUserId = tempUserId
        
        // Add entry in Redis
        RedisUtils.addToSet(activeUserSetRedis, tempUserId.toString)
      }
    }
    
    // Return new user details from database
    return UserEventsDao.getUserDetailsFromRdbms(newUserId)
  }
    
}