package de.kp.spark.elastic
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

import akka.actor.ActorRef

import kafka.consumer.{Consumer,ConsumerConfig,Whitelist}
import kafka.serializer.DefaultDecoder

class KafkaReader(topic:String,actor:ActorRef) {

  private val props = Configuration.kafka
  private val connector = Consumer.create(new ConsumerConfig(props))

  private val stream = connector.createMessageStreamsByFilter(new Whitelist(topic),1,new DefaultDecoder(),new DefaultDecoder())(0)

  def shutdown {
    connector.shutdown()
  }

  def read {
	consume(execute)
  }

  private def consume(write:(Array[Byte]) => Unit) = {

    for (compose <- stream) {
      try {        
        write(compose.message)
      } catch {
        
        case e: Throwable =>
          if (true) { //this is objective even how to conditionalize on it
            //error("Error processing message, skipping this message: ", e)
          } else {
            throw e
          }
      }
    }      
  }

  private def execute(bytes:Array[Byte]) {
    actor ! new String(bytes)    
  }

}