package de.kp.spark.elastic.apps
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
import de.kp.spark.elastic.EsContext

/**
 * An example of how to extract documents from Elasticsearch
 * and apply a simple SQL statement to the documents
 */
object InsightApp {

  def run() {

    val start = System.currentTimeMillis()
 
    /*
     * Spark specific configuration
     */
    val sparkConf = new Configuration()

    sparkConf.set("spark.executor.memory","1g")
	sparkConf.set("spark.kryoserializer.buffer.mb","256")

	val es = new EsContext(sparkConf)

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
    val subjects = es.query(documents, esConf).filter(row => row.getString(0).contains("Re"))    

    subjects.foreach(subject => println(subject))

    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    es.shutdown
    
  }

}