package de.kp.spark.elastic.stream
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

import scala.util.parsing.json._

import kafka.serializer.StringDecoder

import org.apache.spark.SparkContext._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.kafka._

import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.io.{MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsOutputFormat

import de.kp.spark.elastic.SparkBase

/**
 * EsStream provides base functionality for indexing transformed live streams 
 * from Apache Kafka with Elasticsearch; to appy a customized transformation,
 * the method 'transform' must be overwritten
 */
class EsStream(name:String,conf:HConf) extends SparkBase with Serializable {

  /* Elasticsearch configuration */	
  val ec = getEsConf(conf)               

  /* Kafka configuration */
  val (kc,topics) = getKafkaConf(conf)
  
  def run() {
    
    val ssc = createSSCLocal(name,conf)

    /*
     * The KafkaInputDStream returns a Tuple where only the second component
     * holds the respective message; we therefore reduce to a DStream[String]
     */
    val stream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kc,topics,StorageLevel.MEMORY_AND_DISK).map(_._2)
    /*
     * Inline transformation of the incoming stream by any function that maps 
     * a DStream[String] onto a DStream[String]
     */
    val transformed = transform(stream)
    /*
     * Write transformed stream to Elasticsearch index
     */
    transformed.foreachRDD(rdd => {
      val messages = rdd.map(prepare)
      messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],ec)          
    })
    
    ssc.start()
    ssc.awaitTermination()    

  }
  
  def transform(stream:DStream[String]) = stream
  
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
  
  private def prepare(message:String):(Object,Object) = {
      
    val m = JSON.parseFull(message) match {
      case Some(map) => map.asInstanceOf[Map[String,String]]
      case None => Map.empty[String,String]
    }

    val kw = NullWritable.get
    
    val vw = new MapWritable
    for ((k, v) <- m) vw.put(new Text(k), new Text(v))
    
    (kw, vw)
    
  }

}