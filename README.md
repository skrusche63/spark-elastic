![Dr.Krusche & Partner PartG](https://raw.github.com/skrusche63/spark-elastic/master/images/dr-kruscheundpartner.png)

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

### Topics

#### [Elasticsearch and Spark](#1)

&nbsp;&nbsp;&nbsp;&nbsp;  [K-Means Segmentation by Geo Location (Algorithm)](#1.1)

#### [Write to Elasticsearch using Kafka and Spark Streaming](#2)

&nbsp;&nbsp;&nbsp;&nbsp;  [Count-Min Sketch and Streaming (Algorithm)](#2.1)

#### [Technology Stack](#3)

### License

Spark-ELASTIC is licensed under GPL v3. 

Please feel free to contact us under team@dr-kruscheundpartner.de if you need another license or further support.

### <a name="1"></a>Read from Elasticsearch using Spark

Besides linguistic and semantic enrichment, for data in a search index there is an increasing demand to apply knowledge discovery and
data mining techniques, and even predictive analytics to gain deeper insights into the data and further increase their business value.

One of the key prerequisites is to easily connect existing data sources to state-of-the art machine learning and predictive analytics 
frameworks.

In this project, we give advice how to connect Elasticsearch, a powerful distributed search engine, to Apache Spark and profit from the increasing number of existing machine learning algorithms.

The figure shows the integration pattern for Elasticsearch and Spark from an architectural persepctive and also indicates how to proceed with the enriched content (i.e. the way back to the search index).

![Elasticsearch and Spark](https://raw.githubusercontent.com/skrusche63/spark-elastic/master/images/Elasticsearch%20and%20Spark.png)

The source code below describes a few lines of Scala, that are sufficient to read from Elasticsearch and provide data for further mining 
and prediction tasks:

```Scala

/**
 * Read from ES using inputformat from org.elasticsearch.hadoop;
 * note, that key [Text] specifies the document id (_id) and
 * value [MapWritable] the document as a field -> value map
 */
val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
val docs = source.map(hit => {

  val id = hit._1.toString()
  val dc = toMap(hit._2)
      
  (id,dc)
      
}).collect

```

#### <a name="1.1"></a> K-Means Segmentation by Geo Location

From the data format extracted from Elasticsearch 
```Scala 
RDD[(String,Map[String,String]
```
it is just a few lines of Scala to segment these documents with respect to their geo location (latitude,longitude). To this end, the [K-Means clustering](http://http://en.wikipedia.org/wiki/K-means_clustering) implementation 
of [MLlib](https://spark.apache.org/mllib/) is used:
```Scala

object EsKMeans {

  /**
   * This method segments an RDD of documents clustering the assigned (lat,lon) geo coordinates.
   * The field parameter specifies the names of the lat & lon coordinate fields 
   */
  def segmentByLocation(docs:RDD[(String,Map[String,String])],fields:Array[String],clusters:Int,iterations:Int):RDD[(Int,String,Map[String,String])] = {
    /**
     * Train model
     */
    val vectors = docs.map(doc => toVector(doc._2,fields))   
    val model = KMeans.train(vectors, clusters, iterations)
    /**
     * Apply model
     */
    docs.map(doc => {
      
      val vector = toVector(doc._2,fields)
      (model.predict(vector),doc._1,doc._2)
      
    })
    
  }

  private def toVector(data:Map[String,String], fields:Array[String]):Vector = {
       
    val lat = data(fields(0)).toDouble
    val lon = data(fields(1)).toDouble
      
    Vectors.dense(Array(lat,lon))
   
  }
  
}
```

### <a name="2></a> Write to Elasticsearch using Kafka and Spark Streaming

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

[Apache Kafka](http://kafka.apache.org/) is a distributed publish-subscribe messaging system, that may also be seen as a real-time integration system. For example, Web tracking events are easily sent to Kafka, 
and may then be consumed by a set of different consumers.

In this project, we use Spark Streaming as a consumer and aggregator of e.g. such tracking data streams, and perform a live indexing. As Spark Streaming is also able to directly 
compute new insights from data streams, this data integration pattern may be used as a starting point for real-time data analytics and enrichment before search indexing.

The figure below illustrates the architecture of this pattern. For completeness reasons, [Spray](http://spray.io/) has been introduced. Spray is an open-source toolkit for 
building REST/HTTP-based integration layers on top of Scala and Akka. As it is asynchronous, actor-based, fast, lightweight, and modular, it is an easy way to connect Scala 
applications to the Web.

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

#### <a name="2.1"></a> Count-Min Sketch and Streaming

Using the architecture as illustrated above not only enables to apply Spark to data streams. It also open real-time streams to other data processing libraries such as [Algebird](https://github.com/twitter/algebird) from 
Twitter.  

Algebird brings, as the name indicates, algebraic algorithms to streaming data. An important representative is [Count-Min Sketch](http://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) which enables to compute the most 
frequent items from streams in a certain time window. The code example below describes how to apply the CountMinSketchMonoid (Algebird) to compute the most frequent messages from a Kafka Stream with respect to the messages' classification: 

```Scala

object EsCountMinSktech {
    
  def findTopK(stream:DStream[Message]):Seq[(Long,Long)] = {
  
    val DELTA = 1E-3
    val EPS   = 0.01
    
    val SEED = 1
    val PERC = 0.001
 
    val k = 5
    
    var globalCMS = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC).zero
 
    val clases = stream.map(message => message.clas)
    val approxTopClases = clases.mapPartitions(clases => {
      
      val localCMS = new CountMinSketchMonoid(DELTA, EPS, SEED, PERC)
      clases.map(clas => localCMS.create(clas))
    
    }).reduce(_ ++ _)

    approxTopClases.foreach(rdd => {
      if (rdd.count() != 0) globalCMS ++= rdd.first()
    })
        
    /**
     * Retrieve approximate TopK classifiers from the provided messages
     */
    val globalTopK = globalCMS.heavyHitters.map(clas => (clas, globalCMS.frequency(clas).estimate))
      /*
       * Retrieve the top k message classifiers: it may also be interesting to 
       * return the classifier frequency from this method, ignoring the line below
       */
      .toSeq.sortBy(_._2).reverse.slice(0, k)
  
    globalTopK
    
  }
}

```

### <a name="3"></a> Technology Stack

* [Scala](http://scala-lang.org)
* [Apache Kafka](http://kafka.apache.org/)
* [Apache Spark](http://spark.apache.org)
* [Spark Streaming](https://spark.apache.org/streaming/)
* [Twitter Algebird](https://github.com/twitter/algebird)
* [Elasticsearch](http://elasticsearch.org)
* [Elasticsearch Hadoop](http://elasticsearch.org/overview/hadoop/)
* [Spray](http://spray.io/)
