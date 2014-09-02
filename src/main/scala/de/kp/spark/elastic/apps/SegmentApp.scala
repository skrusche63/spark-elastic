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
 * and apply KMeans clustering algorithm to group documents
 * by similar features
 */
object SegmentApp {

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
    
    esConf.set("es.resource", "visits/pageview")                
    esConf.set("es.query", "?q=*:*")                          

    esConf.set("es.fields", "lat,lon")
    
    esConf.set("es.clusters", "10")
    esConf.set("es.iterations", "100")
    
    /*
     * Read from Elasticsearch and apply KMeans clustering
     * to the extracted documents
     */
    val documents = es.documents(esConf)
    val clustered = es.cluster(documents, esConf)  

    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    es.shutdown
    
  }


}