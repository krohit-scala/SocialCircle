package com.socialcircle.utils

import java.io.InputStream
import java.io.FileInputStream
import java.util.Properties

object PropertyFileUtils {
  // Read properties file for input files
  val inputFile : InputStream = new FileInputStream("application.properties")
  val properties = new Properties()
  
  // Get the required property from the property
  def getPropertyFromFile(propertyName : String) : String = {
    properties.load(inputFile)
    properties.getProperty(propertyName)
  }
}