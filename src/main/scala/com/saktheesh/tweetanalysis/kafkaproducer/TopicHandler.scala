package com.saktheesh.tweetanalysis.kafkaproducer

import java.util.Properties

import akka.actor.Actor
import kafka.common.FailedToSendMessageException
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class TopicHandler(brokers: String) extends Actor {

  Console.println("Actor:" + self.path.name + " Created")

  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "sync")
  props.put("auto.create.topics.enable", "true")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val topics = Seq(self.path.name)

  def receive() = {
    case data: KeyedMessage[String, String] if true => {
      try {
        producer.send(data)
        Console.println("sent to" + self.path.name + "(Actor) topic successfully")
      } catch {
        case e: FailedToSendMessageException =>
          e.printStackTrace
          System.exit(1)
        case e: Exception =>
          e.printStackTrace
          System.exit(1)
      }
    }

    case _ => Console.println("unknown Topic")
  }
}
