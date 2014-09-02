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

import java.util.UUID
import org.apache.hadoop.conf.{Configuration => HConf}

object MessageApp {

  val task = "index" // prepare
  
  def main(args:Array[String]) {
    
    val conf = new HConf()
    
    conf.set("es.nodes","localhost")
    conf.set("es.port","9200")
    
    conf.set("es.resource","kafka/messages")              

    conf.set("es.index","kafka")
    conf.set("es.mapping","messages")
        
    conf.set("es.server","http://localhost:9200")

    conf.set("spark.master","local")
    conf.set("spark.batch.duration","15")
      
    conf.set("kafka.topics","publisher")
    conf.set("kafka.threads","1")
      
    conf.set("kafka.group",UUID.randomUUID().toString)
    conf.set("kafka.zklist","127.0.0.1:2181")
      
    // in milliseconds
    conf.set("kafka.timeout","10000")
    
    task match {
      
      case "prepare" => 
    
        val action = "prepare"
        MessageEngine.execute(action,conf)
      
      case "index" => 
    
        val engine = new KafkaEngine("KafkaEngine",conf)
        engine.run

      case _ => {}
      
    }    
    
  }
  
}