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

import java.util.UUID

object MessageApp {

  val task = "index" // prepare
  
  def main(args:Array[String]) {
       
    val settings = Map(
        
      "es.nodes" -> "localhost",
      "es.port"  -> "9200",
    
      "es.resource" -> "kafka/messages",              

      "es.index"   -> "kafka",
      "es.mapping" -> "messages",
        
      "es.server"  -> "http://localhost:9200",

      "spark.master" -> "local",  
      "spark.batch.duration" -> "15",
      
      "kafka.topics"  -> "publisher",
      "kafka.threads" -> "1",
      
      "kafka.group" -> UUID.randomUUID().toString,
      "kafka.zklist" -> "127.0.0.1:2181",
      
      // in milliseconds
      "kafka.timeout" -> "10000"
      
    )
    
    task match {
      
      case "prepare" => 
    
        val action = "prepare"
        MessageEngine.execute(action,settings)
      
      case "index" => 
    
        val engine = new KafkaEngine(settings)
        engine.run

      case _ => {}
      
    }    
    
  }
  
}