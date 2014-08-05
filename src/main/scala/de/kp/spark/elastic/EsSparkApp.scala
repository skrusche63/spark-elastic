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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.serializer.KryoSerializer

import org.apache.hadoop.conf.Configuration

/**
 * An example of how to read ES documents from Spark using org.elasticsearch.hadoop 
 */
object EsSparkApp {

  def main(args:Array[String]) {

    val start = System.currentTimeMillis()
 
    val sc = createCtx()
    /**
     *  Configure access parameters
     */	
    val conf = new Configuration()                          

    conf.set("es.nodes","localhost")
    conf.set("es.port","9200")
    
    conf.set("es.resource", "enron/mails")                
    conf.set("es.query", "?q=*:*")                          

    /**
     * Read from ES and provide some insight with Spark & SparkSQL
     */
    val docs = EsReader.read(sc,conf)
    EsInsight.insight(sc, docs)
    
    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    sc.stop
    
  }
  
  private def createCtx():SparkContext = {

	System.setProperty("spark.executor.memory", "1g")
		
	val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName("EsSparkApp");
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }

}

