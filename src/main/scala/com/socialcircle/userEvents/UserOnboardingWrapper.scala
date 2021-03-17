package com.socialcircle.userEvents

import com.socialcircle.producer.KafkaProducerApi
import com.socialcircle.utils.JsonUtils
import com.socialcircle.utils.PropertyFileUtils
import com.socialcircle.dtos.User

object UserOnboardingWrapper {
  // Perform user onboarding periodically and push it to Kafka
  def onboardAndPushUser() = {
    // Method to onboard a new user from database
    val newUser : User = OnboardNewUser.newUserOnboarding 
    
    // Convert the User object to json before sending to Kafka
    val newUserJson = JsonUtils.getJsonFromObject(newUser)
    
    // Push the new user onboarding event to Kafka
    KafkaProducerApi.sendSingleMessageProducer(PropertyFileUtils.getPropertyFromFile("kafka.new.user.topic"), newUserJson)
    
    // Log message for reference
    println(s"User onboarded: ${newUserJson}")
  }
 
}