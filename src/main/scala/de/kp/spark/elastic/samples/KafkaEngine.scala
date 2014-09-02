package de.kp.spark.elastic.samples
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

import org.apache.spark.streaming.kafka._

import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.{MapWritable,NullWritable,Text}
import org.apache.hadoop.conf.{Configuration => HConf}

import org.elasticsearch.hadoop.mr.EsOutputFormat

import de.kp.spark.elastic.SparkBase

class KafkaEngine(name:String,conf:HConf) extends SparkBase with Serializable {

  /* Elasticsearch configuration */	
  val ec = getEsConf(conf)               

  /* Kafka configuration */
  val (kc,topics) = getKafkaConf(conf)
  
  def run() {
    
    val ssc = createSSCLocal(name,conf)

    val stream = KafkaUtils.createStream[String,Message,StringDecoder,MessageDecoder](ssc,kc,topics, StorageLevel.MEMORY_AND_DISK).map(_._2)
    stream.foreachRDD(messageRDD => {
      /**
       * Live indexing of Kafka messages; note, that this is also
       * an appropriate place to integrate further message analysis
       */
      val messages = messageRDD.map(prepare)
      messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],ec)    
      
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

  
  private def getEsConf(config:HConf):HConf = {
    
    val conf = new HConf()                          

    conf.set("es.nodes", conf.get("es.nodes"))
    conf.set("es.port", conf.get("es.port"))
    
    conf.set("es.resource", conf.get("es.resource")) 
    
    conf
    
  }
  
  private def getKafkaConf(config:HConf):(Map[String,String],Map[String,Int]) = {

    val cfg = Map(
      "group.id" -> conf.get("kafka.group"),
      
      "zookeeper.connect" -> conf.get("kafka.zklist"),
      "zookeeper.connection.timeout.ms" -> conf.get("kafka.timeout")
    
    )

    val topics = conf.get("kafka.topics").split(",").map((_,conf.get("kafka.threads").toInt)).toMap   
    
    (cfg,topics)
    
  }
 
}