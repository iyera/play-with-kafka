/**
 * Created by aiyer on 10/11/15.
 */
package com.aiyer.kafka

import play.api.libs.json.Json

object HeatSensorReadings {
  def main(args: Array[String]): Unit = {
    val topicName = args(0)

    println("Lets get the party started")

    val producer = Producer[String](topicName)
    val message = Message(2, 45.0, "Lets start you off with a light load")

    implicit val writes = Json.writes[Message]

    var counter = 0
    while (true) {

      val message = Message((Math.random()*10000).toInt, Math.random()*200, "Some text to increase message size. Lets fill those pipes up!!")

      implicit val writes = Json.writes[Message]

      counter += 1

      val jsonEvent = Json.toJson(message).toString
      if(counter % 5000 == 0) {
        println("Lets write an event to Kafka: " + jsonEvent)
        counter = 0
      }
      producer.send(jsonEvent )
      //Thread.sleep(1)
    }
  }
}

case class Message(sensorId: Int, temp: Double, desc: String)