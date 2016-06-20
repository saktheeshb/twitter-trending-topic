package com.saktheesh.tweetanalysis.kafkaproducer

import akka.actor.ActorRef
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener}

class TweetListner(actorRef: ActorRef) {

  def simpleStatusListener = new StatusListener() {
    override def onStatus(status: Status) = {
      actorRef ! TweetData(status.getText)
    }

    override def onDeletionNotice(arg0: StatusDeletionNotice){}
    override def onScrubGeo(arg0:Long, arg1:Long){}
    override def onException(org0:Exception){}
    override def onTrackLimitationNotice(org0:Int){}
    override def onStallWarning(arg0:StallWarning ){}

  }
}
