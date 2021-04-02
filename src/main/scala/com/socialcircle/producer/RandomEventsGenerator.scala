package com.socialcircle.producer

import com.socialcircle.userEvents.FollowingEvents
import com.socialcircle.userEvents.UserOnboardingWrapper

object RandomEventsGenerator {
  
  // Generate events periodically
  def triggerEvents = {
    // Onboard new user and push to Kafka
    UserOnboardingWrapper.onboardAndPushUser()
    
    // Make two random user follow existing active users
    FollowingEvents.simulateUserFollowing
    FollowingEvents.simulateUserFollowing
    
    // Make one random user unfollow an existing following
    FollowingEvents.simulateUserUnfollowing
  }

  def main(args: Array[String]): Unit = {
    
    // Application starts
    println(s"Let's socialize...")
    
    // Timer task to trigger events every 3 seconds
    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = triggerEvents
    }

    t.schedule(task, 1000L, 3000L)
    //task.cancel() // Uncommenting this would make the timer task work just once
  }
}