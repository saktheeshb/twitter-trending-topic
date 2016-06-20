# twitter-trending-topic using twitter4j-akka-streaming+akka-actor+Kafka+spark-streaming+kafka-sparkstreaming-direct-api+Cassandra

usage:
______

1.Download Kafka

2.Start zookeeper and Kafka

3.run com.saktheesh.tweetanalysis.kafkaproducer.TweetProducer with arguements #topic1,#topic2,#topic3 localhost:9092 (zookeeper)

4.Above process will pull tweets corresponding to the mentioned topics. Topics will be handled by akka streaming (source,sink based on the configured buffer size). this is to avoid message loss in the kafka producer. These messages will be grouped based on topic and written into kafka. Each topic is handled by an akka actor to write into kafka.

5.run com.saktheesh.tweetanalysis.sparkkafkaconsumer.SparkKafkaConsumer with arguements .topic1,.topic2,.topic3 localhost:9092  (Note here topic name should have . instead of # as kafka topic can't have #.

6.above process will consume messages from kafka in real-time and spark stream would process that to find the trending topics. This process expects a text file which will specify the list of topics and it's corresponding topic group. The final trending output is for the topic group not for the topic. 

for example 
 topic1 with count 10 and topic2 with count 20 belongs to group1 then group1->30 will be the output.(this will help to group similar topics )
 
 format for the text file topic,topicgroup. currently the path of the file is /temp/topicgroup.txt
 

7. the final output will be stored in Cassandra CQL (time series fashion). Any BI tool can be connected to Cassandra and query using Cassandra Query Language
