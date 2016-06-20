package com.saktheesh.tweetanalysis.kafkaproducer

import akka.actor.ActorRef
import twitter4j.FilterQuery


object TwitterHandler {
   def apply(actorRef:ActorRef,topic:String)={
    val twitterClient=TwitterClient()
    val filterQuery = new FilterQuery()
    filterQuery.track(topic)
    twitterClient.addListener( new TweetListner(actorRef).simpleStatusListener)
    twitterClient.filter(filterQuery)
  }
}
