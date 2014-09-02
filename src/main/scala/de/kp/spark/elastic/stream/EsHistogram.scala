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

import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import org.apache.hadoop.conf.{Configuration => HConf}

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import com.twitter.algebird._

class EsHistogram(field:String,conf:HConf) extends EsStream("EsHistogram",conf) {
  
  override def transform(stream:DStream[String]):DStream[String] = {
    histogram(stream,field)
  }
  
  private def histogram(stream:DStream[String],field:String):DStream[String] = {
    
    implicit val formats = DefaultFormats    
    
    /* Mapify stream */
    val mapified = stream.map(json => {
      
      JSON.parseFull(json) match {
      
        case Some(map) => map.asInstanceOf[Map[String,String]]
        case None => Map.empty[String,String]
      
      }
      
    })

    /* Extract field values and compute support for each field value */
    val values = mapified.map(m => m(field))
    val support = values.map(v => (v, 1)).reduceByKey((a, b) => a + b)

    /* The data type of the field value is a String */
    var global = Map[String,Int]()
    val monoid = new MapMonoid[String, Int]()    
    
    /* Determine Top K */
    support.foreachRDD(rdd => {
      
      if (rdd.count() != 0) {
        val partial = rdd.collect().toMap
        global = monoid.plus(global.toMap, partial)
      }
    
    })
    
    mapified.transform(rdd => {
      
      rdd.map(m => {
        
        val v = m(field)
        val s = global(v)
        
        write(m ++ Map("_field" -> field, "_valu" -> v, "_supp" -> s.toString))
        
      })
      
    })
    
  }
  
}