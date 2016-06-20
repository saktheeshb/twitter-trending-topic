package com.saktheesh.tweetanalysis.sparkkafkaconsumer

import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._

case class topicGroup(topic_name: String, group_name: String)
case class topicInfo(topic_name:String, count: Double)
//case class DataFrameRecord(topic: String, date: String, event_time: String, count: Double, percentage: Double)
case class DataFrameRecord(topic:String,date:String,event_time:String,count:Double,percentage:Double)


object SparkKafkaConsumer {

  def main(args: Array[String]) {
    val topics = args(0)
    val brokers = args(1)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Initializing spark,stream and sql contexts
    val sparkConf = new SparkConf().setAppName("TrendingTweetTopic").setMaster("local[2]")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    import sqlContext.implicits._

    //Fetch topic group information from a CSV file with format (topic,topic group)
    val metaDataRDD=ssc.sparkContext.textFile("/tmp/topicgroup.txt")
    val mapTopicGroup=metaDataRDD.map(_.split(",")).map(word => topicGroup(word(0),word(1))).toDF
    mapTopicGroup.registerTempTable("topic_group")


    //Parse input topic List
    val topicsSet = topics.split(",").toSet

    //Read twitter message from kafka stream (Direct read)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    ssc.checkpoint("/tmp/checkpoints/")

    //Topic count for each topic and maintain the state using updateStateByKey
    val topic = messages.map(_._1).map(topicRecord => (topicRecord, 1))
    val topicCount=topic.updateStateByKey(updateFunc)


    //Calculate topic group count based on topic. eg: topic1 10 and topic2 20 belongs to group1 then group1->30
    val topicObject=topicCount.map(topicRecord=>topicInfo(topicRecord._1,topicRecord._2))
    topicObject.foreachRDD(row=>row.foreach(println))
    topicObject.foreachRDD(rdd=>{
      rdd.toDF().registerTempTable("topic_info")
      var result=sqlContext.sql("select group_name,sum(count) as counter from (select group_name,count from topic_info x,topic_group y where x.topic_name=y.topic_name) A group by group_name")
      result.registerTempTable("group_topic_count")
      result=sqlContext.sql("select sum(counter) as total from group_topic_count")
      result.registerTempTable("total_count")

      val now = Calendar.getInstance().getTime()
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ssZZ")
      val dayFormat = new SimpleDateFormat("yyyy-MM-dd")

     result=sqlContext.sql("select group_name as topic,'"+dayFormat.format(now)+"' as date,'"+timeFormat.format(now)+"' as event_time,counter as count,((counter/total) * 100) as percentage " +
        "from  group_topic_count,total_count")

      result.show()

      //Save the result into Cassandra
      result.rdd.map(p => DataFrameRecord(p.getAs[String]("topic"), p.getAs[String]("date"),p.getAs[String]("event_time"),p.getAs[Double]("count"),p.getAs[Double]("percentage"))).saveToCassandra("tweet","trending_topic")

    })
    ssc.start();
    ssc.awaitTermination();
  }

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
}
