package com.socialcircle.producer


import java.util.Properties
import com.google.gson.Gson
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.Callback
import com.socialcircle.utils.PropertyFileUtils


// Callback class for asynchronous producer
class MyProducerCallBack extends Callback{
  @Override
  def onCompletion(recordMetadata : RecordMetadata, exception : Exception) : Unit = {
    if(exception != null){
      println("Asynchronous Producer failed with exceptions!")
      exception.printStackTrace()
    }
  }
}

// Main Kafka producer apis
object KafkaProducerApi {
	val props = new Properties()
	props.put("bootstrap.servers", PropertyFileUtils.getPropertyFromFile("bootstrap.servers"))
	props.put("key.serializer", PropertyFileUtils.getPropertyFromFile("key.serializer"))
	props.put("value.serializer", PropertyFileUtils.getPropertyFromFile("value.serializer"))
  props.put("max.in.flight.requests.per.connection", PropertyFileUtils.getPropertyFromFile("max.in.flight.requests.per.connection"))
    
	// Asynchronous Producer for multiple messages - Doesn't wait for Acknowledgement, retries only for failures.
	def sendSingleMessageProducer(topicName : String, jsonData : String) : Boolean = {
    val props = this.props
    
    // Captures success/failure of the send attempt
    var flag : Boolean = true
	  
	  // Create an instance of Kafka producer 
	  val producer : Producer[String, String] = new KafkaProducer(props);
    try{
	    val record = new ProducerRecord[String, String](topicName, jsonData)
	    producer.send(record, new MyProducerCallBack())
	  }
	  catch{
	    case e : Exception => {
	        flag = false
	        e.printStackTrace()
	    }
	  }
	  finally{
	    producer.close()
	  }
	  flag
	}
	
	// Asynchronous Producer for multiple messages - Doesn't wait for Acknowledgement, retries only for failures.
	def sendMultipleMessagesProducer(topicName : String, dataList : Array[String]) : Boolean = {
    val props = this.props
    
    // Captures success/failure of the send attempt
	  var flag : Boolean = true
	  
	  // Create an instance of Kafka producer
	  val producer : Producer[String, String] = new KafkaProducer(props);

    try{
	    for(jsonData <- dataList){
  	    val record = new ProducerRecord[String, String](topicName, jsonData)
  	    producer.send(record, new MyProducerCallBack())
  	  }
	  }
	  catch{
	    case e : Exception => {
	        flag = false
	        e.printStackTrace()
	    }
	  }
	  finally{
	    producer.close()
	  }
	  flag
	}
}