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

import de.kp.spark.elastic.kafka.Message

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