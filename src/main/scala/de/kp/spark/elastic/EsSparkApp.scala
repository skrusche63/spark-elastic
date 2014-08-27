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

import org.apache.hadoop.conf.Configuration

/**
 * An example of how to read ES documents from Spark using org.elasticsearch.hadoop 
 */
object EsSparkApp {

  def main(args:Array[String]) {

    val start = System.currentTimeMillis()
 
    /*
     * Spark specific configuration
     */
    val sparkConf = new Configuration()

    sparkConf.set("spark.executor.memory","1g")
	sparkConf.set("spark.kryoserializer.buffer.mb","256")

	val es = new ElasticContext(sparkConf)

    /*
     * Elasticsearch specific configuration
     */
    val esConf = new Configuration()                          

    esConf.set("es.nodes","localhost")
    esConf.set("es.port","9200")
    
    esConf.set("es.resource", "enron/mails")                
    esConf.set("es.query", "?q=*:*")                          

    esConf.set("es.table", "docs")
    esConf.set("es.sql", "select subject from docs")
    
    /*
     * Read from ES and provide some insight with Spark & SparkSQL,
     * thereby mixing SQL and other Spark operations
     */
    val documents = es.documentsAsJson(esConf)
    val subjects = es.queryTable(documents, esConf).filter(row => row.getString(0).contains("Re"))    

    subjects.foreach(subject => println(subject))

    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    es.shutdown
    
  }
 
}

