package com.socialcircle.utils

import redis.clients.jedis.Jedis

object RedisUtils {
  // Get IP address of Redis server and make connection
  val redisServerIp = PropertyFileUtils.getPropertyFromFile("redis.server.ip")
  val jedis = new Jedis(redisServerIp)
  
  // Method to ping remote server to check connection
  def pingRedisServer : Unit = {
    try{
      System.out.println("Establishing connection...");
      System.out.println("The server is running " + jedis.ping());
    }
    catch{
      case e : Exception => {
        println(s"Exception occurred!")
        e.printStackTrace
      }
    }
  }
  
  // Method to check if an item belongs to a set
  def checkItemInSet(setName: String, data: String) : Boolean = {
    jedis.sismember(setName, data)
    // println(s"Item added")
  }
  
  // Method to add an item to a set
  def addToSet(setName: String, data: String) : Unit = {
    jedis.sadd(setName, data)
    println(s"Item added: " + data)
  }
}