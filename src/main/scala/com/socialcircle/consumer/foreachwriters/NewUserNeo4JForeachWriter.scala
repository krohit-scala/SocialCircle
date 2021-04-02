package com.socialcircle.consumer.foreachwriters

import org.apache.spark.sql.{ Row, ForeachWriter }
import java.io.Serializable

//import org.neo4j.driver.v1._
import org.neo4j.driver.{Driver, GraphDatabase, AuthTokens, Result, Session}

import scala.collection.JavaConversions._
import java.util.ArrayList
import java.util.List

import com.socialcircle.dtos.User
import com.socialcircle.utils.Neo4JUtils
import com.socialcircle.utils.JsonUtils
import scala.tools.nsc.interpreter.ISettings
import com.socialcircle.utils.Neo4JUtils


// class NewUserNeo4JForeachWriter (uri: String, user: String, password: String) extends ForeachWriter[String] with Serializable {
class NewUserNeo4JForeachWriter extends ForeachWriter[String] with Serializable {

  @Override
  def open(partitionId: Long, version: Long): Boolean = {
    // Open connection
    true
  }

  @Override
  def process(jsonString: String) : Unit = {
    if(jsonString != null && jsonString != ""){
      val newUser = JsonUtils.getUserFromJson(jsonString)
      Neo4JUtils.insertUser(newUser)
    }
  }
  
  @Override
  def close(errorOrNull: Throwable): Unit = {
    // Close the connection
    // neo4jdriver.close()
  }

}