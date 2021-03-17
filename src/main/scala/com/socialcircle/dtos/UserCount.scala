package com.socialcircle.dtos

// Read user followers and followings
// Followers: Number of people following you
// Followings: Number of people you follow

case class UserCount (userId: String, followers: String, followings: String)