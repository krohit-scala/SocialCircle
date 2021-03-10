package com.socialcircle.utils

import com.socialcircle.dtos.User

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet

trait DBUtils {
  // Get properties from property file
  val driver = PropertyFileUtils.getPropertyFromFile("driver")
  val url = PropertyFileUtils.getPropertyFromFile("url")
  val username = PropertyFileUtils.getPropertyFromFile("username")
  val password = PropertyFileUtils.getPropertyFromFile("password")
  
  def executeQueryInMysql(query: String) : Option[ResultSet] = {
    // Connection object declaration
    var connection : Connection = null

    // Resultset for the query 
    var resultSet : ResultSet = null

    try {
      // Make the connection
      // Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // Create the statement, and run the select query
      val statement = connection.createStatement()
      // println(query)
      
      resultSet = statement.executeQuery(query)
    } 
    catch {
      case e : Throwable => e.printStackTrace
      return null
    }
    finally{
      // Close connection to database
      connection.close()
    }
    
    // Return the result
    Option(resultSet)
  }
}