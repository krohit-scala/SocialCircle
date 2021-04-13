package com.socialcircle.utils


import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

import java.io.Serializable

import org.neo4j.driver.{ Driver, GraphDatabase, AuthTokens, Result, Session }
import com.socialcircle.consumer.foreachwriters.NewUserNeo4JForeachWriter
import com.socialcircle.dtos.User
import com.google.gson.Gson

object Neo4JUtils extends Serializable {

  def initNeo4jDriverSession(): Session = {
    // Instantiate the driver
    val neo4jdriver = GraphDatabase.driver(
      PropertyFileUtils.getPropertyFromFile("neo4j.server.url"), 
      AuthTokens.basic(
        PropertyFileUtils.getPropertyFromFile("neo4j.username"), PropertyFileUtils.getPropertyFromFile("neo4j.password")
      )
    )
    
    // Create the session
    val session = neo4jdriver.session
    
    // Return the session object
    session
  }


  // Insert User object in Neo4J
  def insertUser(user: User): Int = {
    val session = initNeo4jDriverSession
    val genderLabel : String = if(user.gender == "Male" || user.gender == "male") "Male" else "Female"
    
      val script : String = s"""
      |CREATE (
      |  user:DummyUser:${genderLabel} {
      |    a: "U${user.userId}",
      |    userId : "${user.userId}",
      |    cityId : "${user.cityId}",
      |    age : ${user.age},
      |    gender : "${user.gender}",
      |    state : "${user.state}",
      |    ts : "${user.ts}",
      |    isActive : ${user.isActive}
      |  }
      |)
      """.stripMargin
      
    val script1 = s"""
      |CREATE(
      |  user:ActiveUsers ${JsonUtils.getJsonFromObject(user).replaceAll("\"(\\w+)\":", "$1:")}
      |)
      """.stripMargin
    
    // "script1" is equivalent to "script"
    // println(s"${script1}")
    
    val result: Result = session.run(script1)
    session.close()
    result.consume().counters().nodesCreated()
  }

  // Insert User Following Relation in Neo4J
  def addNewFollowing(row: Row): Int = {
    val session = initNeo4jDriverSession

    val user1 = row(0).toString // UserId1
    val user2 = row(1).toString // UserId2
    val ts = row(2).toString    // timestamp in long format
    
    // Create the cypher query to add relation
    val script : String = s"""
      |MATCH (u1:ActiveUsers), (u2:ActiveUsers)
      |WHERE u1.userId=${user1} AND u2.userId=${user2}
      |CREATE (u1)-[:FOLLOWS {since_ts: ${ts}} ]->(u2)
      """.stripMargin
      
    val result: Result = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()
  }
  
  // Insert User Unfollowing Relation in Neo4J
  def addNewUnfollowing(row: Row): Int = {
    val session = initNeo4jDriverSession

    val user1 = row(0).toString // UserId1
    val user2 = row(1).toString // UserId2
    val ts = row(2).toString    // timestamp in long format
    
    // Create the cypher query to add relation
    val script : String = s"""
      |MATCH (u1:ActiveUsers)-[r:FOLLOWS]->(u2:ActiveUsers)
      |WHERE u1.userId=${user1} AND u2.userId=${user2}
      |DELETE r
      """.stripMargin
      
    val result: Result = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()
  }
  
  
  
  // General method to write batch dataframe to Neo4J
  def writeData(df: DataFrame, saveMode: SaveMode, labels: String) = {
    df.write.format("org.neo4j.spark.DataSource")
      .mode(saveMode)
      .option("url", PropertyFileUtils.getPropertyFromFile("neo4j.server.url"))
      .option("authentication.basic.username", PropertyFileUtils.getPropertyFromFile("neo4j.username"))
      .option("authentication.basic.password", PropertyFileUtils.getPropertyFromFile("neo4j.password"))
      .option("labels", labels)
      .save
  }
  
  // General method to read label as dataframe from Neo4J
  def readDataByLables(sparkSession: SparkSession, labels: String): DataFrame = {
    val df = sparkSession.read.format("org.neo4j.spark.DataSource")
      .option("url", PropertyFileUtils.getPropertyFromFile("neo4j.server.url"))
      .option("authentication.basic.username", PropertyFileUtils.getPropertyFromFile("neo4j.username"))
      .option("authentication.basic.password", PropertyFileUtils.getPropertyFromFile("neo4j.password"))
      .option("labels", labels)
      .load

    df
  }

  // General method to get dataframe by running queries in Neo4J
  def readDataByQuery(sparkSession: SparkSession, driver: String, url: String, username: String, password: String, query: String): DataFrame = {
    val df = sparkSession.read.format("org.neo4j.spark.DataSource")
      .option("url", PropertyFileUtils.getPropertyFromFile("neo4j.server.url"))
      .option("authentication.basic.username", PropertyFileUtils.getPropertyFromFile("neo4j.username"))
      .option("authentication.basic.password", PropertyFileUtils.getPropertyFromFile("neo4j.password"))
      .option("query", query)
      .load

    df
  }
}

