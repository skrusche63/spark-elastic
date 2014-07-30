package de.kp.spark.elastic.kafka
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-ELASTIC project
* (https://github.com/skrusche63/spark-elastic).
* 
* Spark-ELASTIC is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-ELASTIC is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-ELASTIC. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.kafka._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.io.{MapWritable,NullWritable,Text}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.fs.{FileSystem,Path}

import org.elasticsearch.hadoop.mr.EsOutputFormat

class KafkaEngine(settings:Map[String,String]) extends Serializable {
   
  def run() {

    val sc = createLocalCtx()    
    /**
     * Batch duration is the time duration spark streaming uses to 
     * collect spark RDDs; with a duration of 5 seconds, for example
     * spark streaming collects RDDs every 5 seconds, which then are
     * gathered int RDDs    
     */
    val batch  = settings("spark.batch.duration").toInt    
    val ssc = new StreamingContext(sc, Seconds(batch))

    /**
     * Elasticsearch configuration
     */	
    val esConfig = new Configuration()                          

    esConfig.set("es.nodes", settings("es.nodes"))
    esConfig.set("es.port", settings("es.port"))
    
    esConfig.set("es.resource", settings("es.resource"))               

    /**
     * Kafka configuration
     */
    val kafkaConfig = Map(
      "group.id" -> settings("kafka.group"),
      
      "zookeeper.connect" -> settings("kafka.zklist"),
      "zookeeper.connection.timeout.ms" -> settings("kafka.timeout")
    
    )

    val kafkaTopics = settings("kafka.topics")
      .split(",").map((_,settings("kafka.threads").toInt)).toMap   

    /**
     * The KafkaInputDStream returns a Tuple(String,String) where only the second component
     * holds the respective message; before any further processing is initiated, we therefore
     * reduce to a DStream[String]
     */
    val stream = KafkaUtils.createStream[String,Message,StringDecoder,MessageDecoder](ssc, kafkaConfig, kafkaTopics, StorageLevel.MEMORY_AND_DISK).map(_._2)
    stream.foreachRDD(messageRDD => {
      /**
       * Live indexing of Kafka messages; note, that this is also
       * an appropriate place to integrate further message analysis
       */
      val messages = messageRDD.map(prepare)
      messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],esConfig)    
      
    })
    
    ssc.start()
    ssc.awaitTermination()    

  }
  
  private def prepare(msg:Message):(Object,Object) = {
      
    val m = MessageUtils.messageToMap(msg)

    /**
     * Prepare (Keywritable, ValueWritable)
     */
    val kw = NullWritable.get
    
    val vw = new MapWritable
    for ((k, v) <- m) vw.put(new Text(k), new Text(v))
    
    (kw, vw)
    
  }

  private def createLocalCtx():SparkContext = {

	System.setProperty("spark.executor.memory", "1g")
		
	val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName("KafkaEngine");
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }
 
}