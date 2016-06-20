package com.saktheesh.tweetanalysis.kafkaproducer

import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object TwitterClient {
  // Fill these in with your own credentials
  val appKey: String = "Rj4AHAjIfuT5lq4t28icjISuw"
  val appSecret: String = "kMZQQHcxKerckuQCqJNSsqr5JiauTNMjnYtvHH9nzeJDCw0n78"
  val accessToken: String = "1116551834-PssPwQlR10qnWrqGXO6Gb2vdcdV0fQEHNNx2JzV"
  val accessTokenSecret: String = "si4KauZc05l8GTnvkIGh5gP4BIMckJP0TFey6o7UAhtxd"

  def apply(): TwitterStream = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(appKey)
      .setOAuthConsumerSecret(appSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    new TwitterStreamFactory(cb.build).getInstance()
  }
}
