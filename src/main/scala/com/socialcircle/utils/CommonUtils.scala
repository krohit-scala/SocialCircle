package com.socialcircle.utils

import java.util.Calendar
import java.text.SimpleDateFormat

object CommonUtils {
  
  // Returns the timestamp in long format
  def getTimestampLongFormat : Long = {
    return Calendar.getInstance.getTimeInMillis
  }
  
  // Get date/time from timestamp long format
  def getDateTimeFromTimestamp(pattern: String, ts: Long) : String = {
    val format = new SimpleDateFormat(pattern)
    format.format(pattern, ts)
  }
  
}