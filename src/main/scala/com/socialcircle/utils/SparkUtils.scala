package com.socialcircle.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SparkUtils {
  
  // Get or create SparkSession
  def getSparkSession : SparkSession = {
    // Read Spark configurations from Property file
    val master = PropertyFileUtils.getPropertyFromFile("master")
    val appName = PropertyFileUtils.getPropertyFromFile("app.name")
    val sparkSqlShufflePartition = PropertyFileUtils.getPropertyFromFile("spark.sql.shuffle.partition")
    val esIndexAutoCreate = PropertyFileUtils.getPropertyFromFile("es.index.auto.create")
    val esNodes = PropertyFileUtils.getPropertyFromFile("es.nodes")
    val esNodesDiscovery = PropertyFileUtils.getPropertyFromFile("es.nodes.discovery")
    val esNodesDataOnly = PropertyFileUtils.getPropertyFromFile("es.nodes.data.only")
    val sparkCassandraConnectionHost = PropertyFileUtils.getPropertyFromFile("spark.cassandra.connection.host")
    val sparkCassandraConnectionPort = PropertyFileUtils.getPropertyFromFile("spark.cassandra.connection.port")
    val sparkCassandraConnectionKeepAliveMs = PropertyFileUtils.getPropertyFromFile("spark.cassandra.connection.keep_alive_ms")
    val sparkRedisHost = PropertyFileUtils.getPropertyFromFile("spark.redis.host")
    val sparkRedisPort = PropertyFileUtils.getPropertyFromFile("spark.redis.port")
    
    // Create the SparkConf object
    val conf = new SparkConf()
            .setMaster(master)
            .setAppName(appName)
            .set("spark.sql.shuffle.partition", sparkSqlShufflePartition)
            .set("es.index.auto.create", esIndexAutoCreate)          // Elasticsearch configs
            .set("es.nodes", esNodes)                                // Elasticsearch configs
            .set("es.nodes.discovery", esNodesDiscovery)             // Elasticsearch configs
            .set("es.nodes.data.only", esNodesDataOnly)              // Elasticsearch configs
            .set("spark.cassandra.connection.host", sparkCassandraConnectionHost)                 // Cassandra configs
            .set("spark.cassandra.connection.port", sparkCassandraConnectionPort)                 // Cassandra configs
            .set("spark.cassandra.connection.keep_alive_ms", sparkCassandraConnectionKeepAliveMs) // Cassandra configs
            .set("spark.redis.host", sparkRedisHost)         // Redis configs
            .set("spark.redis.port", sparkRedisPort)         // Redis configs
    
    // Get or create the Spark Session object
    val spark = SparkSession.builder
                .config(conf)
                .getOrCreate

    spark.sparkContext.setLogLevel("WARN")
    
    spark.conf.set("spark.sql.shuffle.partitions", sparkSqlShufflePartition)
    spark
  }

  // Set properties to SparkSession object
  def addPropertyToSparkConf(property: String, value: String) : Unit = {
    getSparkSession.conf.set(property, value)
  }
  
  // Close SparkSession
  def closeSparkSession : Unit = {
    getSparkSession.stop
    getSparkSession.close
  }
}