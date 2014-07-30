## Integration of Elasticsearch with Apache Spark

This project shows how to easily integrate [Apache Spark](http://spark.apache.org), a fast and general purpose engine for 
large-scale data processing, with [Elasticsearch](http://elasticsearch.org), a real-time distributed search and analytics 
engine.

Spark is an in-memory processing framework and outperforms Hadoop up to a factor of 100. Spark is accompanied by 

* [MLlib](https://spark.apache.org/mllib/), a scalable machine learning library,
* [Spark SQL](https://spark.apache.org/sql/), a unified access platform for structured big data,
* [Spark Streaming](https://spark.apache.org/streaming/), a library to build scalable fault-tolerant streaming applications.

Combining Apache Spark and Elasticsearch brings the power of machine learning, real-time data sources such as social media and 
more to an Enterprise Search Platform. 

### Read from Elasticsearch using Spark

TBD

### Write to Elasticsearch using Kafka and Spark Streaming

Real-time analytics is a very popular topic with a wide range of application areas:

* High frequency trading (finance), 
* Real-time bidding (adtech), 
* Real-time social activity (social networks),
* Real-time sensoring (Internet of things),
* Real-time user behavior,

and more, gain tremendous business value from real-time analytics. There exist a lot of popular frameworks to aggregate data in real-time, such as Apache Storm, 
Apache S4, Apache Samza, Akka Streams, SQLStream to name just a few.

Spark Streaming, which is capable to process about 400,000 records per node per second for simple aggregations on small records, significantly outperforms other popular 
streaming systems. This is mainly because Spark Streaming groups messages in small batches which are then processed together. 

Moreover in case of failure, Spark Streaming batches are only processed once which greatly simplifies the logic (e.g. to make sure some values are not counted multiple times).

Spark Streaming is a layer on top of Spark and transforms and batches data streams from various sources, such as Kafka, Twitter or ZeroMQ into a sequence of 
Spark RDDs (Resilient Distributed DataSets) using a sliding window. These RDDs can then be manipulated using normal Spark operations.

This project provides a real-time data integration pattern based on Apache Kafka, Spark Streaming and Elasticsearch: 

Apache Kafka is a distributed publish-subscribe messaging system, that may also be seen as a real-time integration system. For example, Web tracking events are easily sent to Kafka, 
and may then be consumed by a set of different consumers.

In this project, we use Spark Streaming as a consumer and aggregator of e.g. such tracking data streams, and perform a live indexing. As Spark Streaming is also able to directly 
compute new insights from data streams, this data integration pattern may be used as a starting point for real-time data analytics and enrichment before search indexing.

The figure below illustrates the architecture of this pattern.

![Real-time Data Integration and Analytics](https://raw.github.com/skrusche63/spark-elastic/master/images/Real-time%20Data%20Integration%20and%20Analytics.png)

The code example below illustrates that such an integration pattern may be implemented with just a few lines of Scala code:

```Scala

val stream = KafkaUtils.createStream[String,Message,StringDecoder,MessageDecoder](ssc, kafkaConfig, kafkaTopics, StorageLevel.MEMORY_AND_DISK).map(_._2)
stream.foreachRDD(messageRDD => {
  /**
   * Live indexing of Kafka messages; note, that this is also
   * an appropriate place to integrate further message analysis
   */
  val messages = messageRDD.map(prepare)
  messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],esConfig)    
      
})

```
