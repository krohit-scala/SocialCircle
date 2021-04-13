package com.socialcircle.consumer.foreachwriters

package com.socialcircle.consumer.foreachwriters

import org.apache.spark.sql.{Row, ForeachWriter }
import java.io.Serializable

import org.neo4j.driver.{Driver, GraphDatabase, AuthTokens, Result, Session}

import scala.collection.JavaConversions._
import java.util.ArrayList
import java.util.List

import com.socialcircle.dtos.User
import com.socialcircle.utils.Neo4JUtils
import com.socialcircle.utils.JsonUtils


// class NewUserNeo4JForeachWriter (uri: String, user: String, password: String) extends ForeachWriter[String] with Serializable {
class NewUnfollowingNeo4JForeachWriter extends ForeachWriter[Row] with Serializable {

  @Override
  def open(partitionId: Long, version: Long): Boolean = {
    // Open connection
    true
  }

  @Override
  def process(row: Row) : Unit = {
    if(row != null){
      Neo4JUtils.addNewUnfollowing(row)
    }
  }
  
  @Override
  def close(errorOrNull: Throwable): Unit = {
    // Close the connection
    // neo4jdriver.close()
  }

}