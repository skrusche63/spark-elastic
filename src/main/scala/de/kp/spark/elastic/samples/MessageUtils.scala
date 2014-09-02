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

import org.apache.hadoop.conf.{Configuration => HConf}
import de.kp.spark.elastic.EsClient

/**
 * Please note, that part of the functionality below is taken from
 * the code base assigned to this blog entry:
 * 
 * http://sujitpal.blogspot.de/2012/11/indexing-into-elasticsearch-with-akka.html
 */

object MessageEngine {
  
  import concurrent.ExecutionContext.Implicits._
    
  private val client = new EsClient()

  private val shards:Int   = 1
  private val replicas:Int = 1
  
  private val es_CreateIndex:String = """
    {"settings": {"index": {"number_of_shards": %s, "number_of_replicas": %s}}}""".format(shards, replicas)
    
  private val es_CreateSchema:String = """{ "%s" : { "properties" : %s } }"""

  private val schema = new MessageSchema()

  def execute(action:String,conf:HConf) {
    
    action match {
        
      case "prepare" => prepare(conf)
        
      case _ => {}
      
    }
    
  }
  
  private def prepare(conf:HConf) {
    
    val index  = conf.get("es.index")
    val server = conf.get("es.server")
    
    /**
     * Create new index
     */
    val server0 = List(server, index).foldRight("")(_ + "/" + _)
    client.post(server0, es_CreateIndex)
   
    /**
     * Create new schema
     */   
    val server1 = List(server, index, conf.get("es.mapping")).foldRight("")(_ + "/" + _)
    client.post(server1 + "_mapping", es_CreateSchema.format(index, schema.mappings))

  }
 
}

object MessageUtils {
   
  def messageToMap(message:Message):Map[String,String] = {
 
    Map(
     "mid"  -> message.mid,
     "text" -> message.text,     
     "timestamp" -> message.timestamp.toString
    )

  }
 
}

/**
 * Specification of sample data structures; the classifier
 * is introduced to support the CountMinSketch algorithm
 */

case class Message(
    mid:String,
    clas:Long,
    text:String,
    timestamp:Long
)

class MessageSchema {
  
  def mappings(): String = """{
    "mid":  {"type": "string", "index": "not_analyzed", "store": "yes"},
    "text": {"type": "string", "index": "analyzed", "store": "yes"},
    "timestamp": {"type": "string", "index": "not_analyzed", "store": "yes"}
  }"""
    
}