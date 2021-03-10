package com.socialcircle.utils

import com.google.gson.Gson
import com.socialcircle.dtos.User

object JsonUtils {
  // Create JSON from objects
  def getJsonFromObject(obj : Object) : String = new Gson().toJson(obj)
  
  // Create object from JSON
  def getUserFromJson(json : String) : User = new Gson().fromJson(json, classOf[User])
  
}