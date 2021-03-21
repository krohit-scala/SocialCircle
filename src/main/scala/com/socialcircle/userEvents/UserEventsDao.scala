package com.socialcircle.userEvents

import java.sql.DriverManager
import com.socialcircle.dtos.User

import java.sql.DriverManager
import java.sql.Connection
import com.socialcircle.utils.DBUtils
import java.sql.ResultSet
import com.socialcircle.utils.DBUtils
import java.util.Calendar

object UserEventsDao extends DBUtils {
/*
  // Fetch user details from database
  def getUserDetailsFromRdbms(userId : Int) : User = {
    var cityId = -1
    var age = -1
    var gender : String = null
    var city : String = null
    var state : String = null
    
    val query = s"""
        |SELECT a.*, b.city, b.state
        |FROM demography.user a 
        |INNER JOIN demography.city b 
        |ON (a.city_id = b.city_id)
        |WHERE a.id=${userId}""".stripMargin
    // println(query)

    // Get data from MySQL
    val resultset : ResultSet = executeQueryInMysql(query).getOrElse(null)
    
    while (resultset != null && resultset.next) {
      val userId1 = Integer.parseInt(resultset.getString("id"))
      cityId = Integer.parseInt(resultset.getString("city_id"))
      age = Integer.parseInt(resultset.getString("age"))
      gender = resultset.getString("gender")
      city = resultset.getString("city")
      state = resultset.getString("state")
      println(s"User_ID: ${userId1}, City_ID: ${cityId}, Age: ${age}, Gender: ${gender}, City: ${city}, State: ${state}")
    }
    
    // Return the user
    return User(userId, cityId, age, gender, city, state)
  }  
*/
  // Fetch user details from database
  def getUserDetailsFromRdbms(userId: Int) : User = {
    var cityId = -1
    var age = -1
    var gender : String = null
    var city : String = null
    var state : String = null
    var ts : Long = Calendar.getInstance.getTimeInMillis
    
    val query = s"""
        |SELECT a.*, b.city, b.state
        |FROM demography.user a 
        |INNER JOIN demography.city b 
        |ON (a.city_id = b.city_id)
        |WHERE a.id=${userId}""".stripMargin

    // Connection object declaration
    var connection : Connection = null

    try {
      // Make the connection
      // Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // Create the statement, and run the select query
      val statement = connection.createStatement()
      // println(query)
      
    // Resultset for the query 
      val resultset : ResultSet = statement.executeQuery(query)
      while (resultset != null && resultset.next) {
        val userId1 = Integer.parseInt(resultset.getString("id"))
        cityId = Integer.parseInt(resultset.getString("city_id"))
        age = Integer.parseInt(resultset.getString("age"))
        gender = resultset.getString("gender")
        city = resultset.getString("city")
        state = resultset.getString("state")
        println(s"User_ID: ${userId1}, City_ID: ${cityId}, Age: ${age}, Gender: ${gender}, City: ${city}, State: ${state}")
      }
    }
    catch {
      case e : Throwable => e.printStackTrace
    }
    finally{
      // Close connection to database
      connection.close()
    }
    
    // Return the result
    User(userId, cityId, age, gender, city, state, ts, true)
  }
}