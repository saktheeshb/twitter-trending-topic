package com.saktheesh.tweetanalysis.kafkaproducer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

object TweetProducer {

  var brokers: String = ""

  def main(args: Array[String]) {

    val topic = args(0)
    val topicList: Set[String] = topic.split(",").toSet
    brokers = args(1)

    val actorRef = new AkkaStreamHandler(topicList, brokers).run()
    TwitterHandler(actorRef, topic)
  }
}

