package com.socialcircle.dtos

case class User (
  userId : Int,
  cityId : Int,
  age : Int,
  gender : String,
  city : String,
  state : String,
  ts: Long,
  isActive: Boolean
)