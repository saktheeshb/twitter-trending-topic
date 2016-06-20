package com.saktheesh.tweetanalysis.kafka.kafkaproducer

import org.scalatest._

abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors

class KafkaTwitterProducerTest extends UnitSpec {

    "function" should  "print hello world" in{
      //  val kafka=  KafkaTwitterProducer
        //assert(kafka.fun()=="hello world")
    }

}
