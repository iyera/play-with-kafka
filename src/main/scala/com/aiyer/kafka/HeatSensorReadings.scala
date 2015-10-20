/**
 * Created by aiyer on 10/11/15.
 */
package com.aiyer.kafka

import play.api.libs.json.Json

object HeatSensorReadings {
  def main(args: Array[String]): Unit = {
    val topicName =
      if (args.length == 0) "HeatSensors"
      else args(0)

    println("Lets get the party started")

    val producer = Producer[String](topicName)
    val message = Message(2, 45.0)

    implicit val writes = Json.writes[Message]

    var counter = 0
    while (true) {

      val message = Message((Math.random()*10000).toInt, Math.random()*200)

      implicit val writes = Json.writes[Message]

      counter += 1

      if(counter % 1000 == 0) {
        println("Lets write an event to Kafka")
        counter = 0
      }
      producer.send(Json.toJson(message).toString)
      Thread.sleep(100)
    }
  }
}

case class Message(sensorId: Int, temp: Double)