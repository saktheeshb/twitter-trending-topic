package com.saktheesh.tweetanalysis.kafkaproducer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import kafka.producer.KeyedMessage


class AkkaStreamHandler(topicList: Set[String], brokers: String) {
  implicit val system = ActorSystem("twitter")
  implicit val materializer = ActorMaterializer()
  var actorsMap = scala.collection.mutable.Map.empty[String, ActorRef]

  createKafkaProducerActorPerTopic()

  def run(): ActorRef = {

    val source = Source.actorRef[TweetData](100000, OverflowStrategy.fail)
    val sink = sinkHandler
    val flow = Flow[TweetData].to(sink)

    flow.runWith(source)
  }


  private def createKafkaProducerActorPerTopic() = {
    topicList.foreach[String](topicName => {
      actorsMap(topicName) = system.actorOf(Props(new TopicHandler(brokers)), name = topicName.replace('#', '.')) //Actor Name can't start with #
      null
    })
  }

  private def sinkHandler() = {
    Sink.foreach[TweetData](record => {
      println("----"+record.tweet+"-----")

      topicList.foreach[String](topicName => {
        if (record.tweet.toLowerCase contains topicName.toLowerCase()) {
          Console.println(topicName)
          val data = new KeyedMessage[String, String](topicName.replace('#', '.'), topicName, record.tweet);
          actorsMap(topicName) ! data
        }
        null
      }
      )
    })
  }

}
