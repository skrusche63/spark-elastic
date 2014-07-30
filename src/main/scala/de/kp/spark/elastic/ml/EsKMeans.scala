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

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

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