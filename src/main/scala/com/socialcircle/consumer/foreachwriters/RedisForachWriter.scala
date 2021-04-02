package com.socialcircle.consumer.foreachwriters

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import org.apache.spark.sql.Dataset

// FOREACHWRITER FOR REDIS
// Generic class that can be used for writing any record as Redis hash 
// provided 0-th element of the record is treated as the id column for that record
class RedisForeachWriter(val host: String, port: String, val hashName: String) extends ForeachWriter[Row]{

  var jedis: Jedis = _

  def connect() = {
    jedis = new Jedis(host, port.toInt)
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    return true
  }

  override def process(record: Row) = {
    val u_id = record.getString(0);
    
    if(!(u_id == null || u_id.isEmpty())){
      val columns : Array[String] = record.schema.fieldNames
  
      if(jedis == null){
        connect()
      }
      
      for(i <- 0 until columns.length){
        if(! ((record.getString(i) == null) || (record.getString(i).isEmpty()) || record.getString(i) == "") )
          jedis.hset(s"${hashName}:" + u_id, columns(i), record.getString(i))
      }
    }
  }

  override def close(errorOrNull: Throwable) = {
    jedis.close
  }
}
