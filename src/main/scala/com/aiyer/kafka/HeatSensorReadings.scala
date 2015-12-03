/**
 * Created by aiyer on 10/11/15.
 */
package com.aiyer.kafka

// Read the "Serialization" section in https://github.com/json4s/json4s
// Case classes can be serialized and deserialized.
import org.json4s.jackson.Serialization

import org.json4s.NoTypeHints

object HeatSensorReadings {

  implicit val formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    val topicName = args(0)

    println("Lets get the party started")

    val producer = Producer[String](topicName)
    val message = SensorReading(2, 45.0, "Lets start you off with a light load")


    var counter = 0
    while (true) {

      val message = SensorReading((Math.random()*10000).toInt, Math.random()*200, "Some text to increase message size. Lets fill those pipes up!!")

      counter += 1

      if(counter % 5000 == 0) {
        println("Lets write an event to Kafka: " + message.sensorId)
        counter = 0
      }
      producer.send(Serialization.write(message))
      //Thread.sleep(1)
    }
  }


}

case class SensorReading(sensorId: Int, temp: Double, desc: String)