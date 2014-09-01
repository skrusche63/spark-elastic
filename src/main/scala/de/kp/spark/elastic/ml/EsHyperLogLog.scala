package de.kp.spark.elastic.ml
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

import com.twitter.algebird._
import org.apache.spark.streaming.dstream.DStream

import de.kp.spark.elastic.stream.Message
import java.nio.ByteBuffer

object EsHyperLogLog {

  def estimateCardinality(stream:DStream[Message]):Double = {

    val BIT_SIZE = 12
    
    val clases = stream.map(message => message.clas)
    val approxClases = clases.mapPartitions(clases => {
      
      /* 12: Number of bits */
      val hll = new HyperLogLogMonoid(12)
      clases.map(clas => {
        
        val bytes = ByteBuffer.allocate(8).putLong(clas).array()
        hll(bytes)
      
      })
    
    }).reduce(_ + _)

    val hll = new HyperLogLogMonoid(BIT_SIZE)
    var globalHll = hll.zero
 
    approxClases.foreach(rdd => {
      if (rdd.count() != 0) {
        globalHll += rdd.first()
      }
    })
 
    /*
     * Approximate distinct clases in the observed messages
     */
    globalHll.estimatedSize

  }
}