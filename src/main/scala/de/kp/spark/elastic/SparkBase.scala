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

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.serializer.KryoSerializer

import org.apache.spark.streaming.{Seconds,StreamingContext}

import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._

trait SparkBase {
  
  protected def createSSCLocal(name:String,config:Configuration):StreamingContext = {

    val sc = createSCLocal(name,config)
    
    /*
     * Batch duration is the time duration spark streaming uses to 
     * collect spark RDDs; with a duration of 5 seconds, for example
     * spark streaming collects RDDs every 5 seconds, which then are
     * gathered int RDDs    
     */
    val batch  = config.get("spark.batch.duration").toInt    
    new StreamingContext(sc, Seconds(batch))

  }
  
  protected def createSCLocal(name:String,config:Configuration):SparkContext = {

    /* Extract Spark related properties from the Hadoop configuration */
    val iterator = config.iterator()
    for (prop <- iterator) {

      val k = prop.getKey()
      val v = prop.getValue()
      
      if (k.startsWith("spark."))System.setProperty(k,v)      
      
    }

    val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName(name);
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
    
    /* Set the Jetty port to 0 to find a random port */
    conf.set("spark.ui.port", "0")        
        
	new SparkContext(conf)
		
  }

  protected def createSSCRemote(name:String,config:Configuration):SparkContext = {
    /* Not implemented yet */
    null
  }

  protected def createSCRemote(name:String,config:Configuration):SparkContext = {
    /* Not implemented yet */
    null
  }

}