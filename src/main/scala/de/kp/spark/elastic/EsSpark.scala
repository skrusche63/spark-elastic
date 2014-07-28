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
import org.apache.hadoop.io.{ArrayWritable,MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsInputFormat

import scala.collection.JavaConversions._
/**
 * An example of how to read ES documents from Spark using org.elasticsearch.hadoop 
 */
object EsSpark {

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
     * Read from ES using inputformat from org.elasticsearch.hadoop;
     * note, that key [Text] specifies the document id (_id) and
     * value [MapWritable] the document as a field -> value map
     */
    val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val docs = source.map(hit => {

      val id = hit._1.toString()
      val dc = toMap(hit._2)
      
      (id,dc)
      
    }).collect

    /**
     * Further computation of ES result; this may be done with or
     * without converting docs to an Array (collect)
     */
    docs.foreach(doc => {
      
      println("ID: " + doc._1)
      println(doc._2)
      
      println("------------------------------")
    
    })
    
    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    sc.stop
    
  }
  /**
   * A helper method to convert a MapWritable into a Map
   */
  private def toMap(mw:MapWritable):Map[String,String] = {
      
    val m = mw.map(e => {
        
      val k = e._1.toString        
      val v = (if (e._2.isInstanceOf[Text]) e._2.toString()
        else if (e._2.isInstanceOf[ArrayWritable]) {
        
          val array = e._2.asInstanceOf[ArrayWritable].get()
          array.map(item => {
            
            (if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString)}).mkString(",")
            
        }
        else "")
        
    
      k -> v
        
    })
      
    m.toMap
    
  }
  
  private def createCtx():SparkContext = {

	System.setProperty("spark.executor.memory", "1g")
		
	val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName("SparkES");
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }

}

