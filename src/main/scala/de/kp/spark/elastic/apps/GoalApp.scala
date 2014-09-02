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

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration

import de.kp.spark.elastic.{EsContext,EsDocument}

import de.kp.spark.elastic.bayes.{ClickModel,ClickTrainer}
import de.kp.spark.elastic.specs.{GoalSpec,PageViewSpec}

object GoalApp {

  def run(clicks:Int,goal:String) {

    val start = System.currentTimeMillis()
 
    /* Configure Apache Spark */
    val sparkConf = new Configuration()

    sparkConf.set("spark.executor.memory","1g")
	sparkConf.set("spark.kryoserializer.buffer.mb","256")

	val es = new EsContext(sparkConf)

    /* Configure Elasticsearch */
    val esConf = new Configuration()                          

    esConf.set("es.nodes","localhost")
    esConf.set("es.port","9200")
    
    esConf.set("es.resource", "visits/pageview")                
    esConf.set("es.query", "?q=*:*")                          

    val fields = PageViewSpec.get.map(_._2._1).mkString(",")
    esConf.set("es.fields", fields)
    
    /*
     * Read from Elasticsearch and restrict to those document fields
     * specified by PageViewSpec 
     */
    val documents = es.documentsFromSpec(esConf)

    /*
     * Extract dataset: (sessionid,timestamp,userid,pageurl,visittime,referrer)
     */
    val extracted = extract(documents,PageViewSpec.get)
    /*
     * Evaluate extracted dataset and determine whether the conversion goal provided matches the 
     * page urls within a session
     * 
     * Evaluated dataset: (sessid,userid,total,starttime,timespent,referrer,exitpage,flowstatus)
     */
    val evaluated = evaluate(extracted,goal)
    /*
     * Train a Bayes model from the evaluated dataset
     */
    val model = ClickTrainer.train(evaluated)
    println("Conversion Probability: " + model.predict(clicks))
    
    val end = System.currentTimeMillis()
    println("Total time: " + (end-start) + " ms")
    
    es.shutdown
    
  }
  
  def evaluate(source:RDD[(String,Long,String,String,String,String)],goal:String):RDD[(String,String,Int,Long,Long,String,String,Int)] = {
 
    /* Group source by sessionid */
    val dataset = source.groupBy(group => group._1)
    dataset.map(valu => {
      
      /* Sort single session data by timestamp */
      val data = valu._2.toList.sortBy(_._2)

      val pages = data.map(_._4)
     
      /* Total number of page clicks */
      val total = pages.size
      
      val (sessid,starttime,userid,pageurl,visittime,referrer) = data.head
      val endtime = data.last._2
      
      /* Total time spent for session */
      val timespent = (if (total > 1) (endtime - starttime) / 1000 else 0)
      val exitpage = pages(total - 1)
      
      /*
       * This is a simple session evaluation to determine whether the sequence of
       * pages per session matches with a predefined page flow
       */
      val flowstatus = GoalSpec.checkFlow(goal,pages)      
      (sessid,userid,total,starttime,timespent,referrer,exitpage,flowstatus)
      
    })
    
  }

  private def extract(documents:RDD[EsDocument],spec:Map[String,(String,String)]):RDD[(String,Long,String,String,String,String)] = {

    val sc = documents.context
    val bspec = sc.broadcast(spec)
    
    documents.map(document => {
      
      /* sessionid */
      val sessionid = document.data(bspec.value("sessionid")._1)
      
      /* timestamp */
      val timestamp = document.data(bspec.value("timestamp")._1).toLong

      /* userid */
      val userid = document.data(bspec.value("userid")._1)
 
      /* pageurl */
      val pageurl = document.data(bspec.value("pageurl")._1)
 
      /* visittime */
      val visittime = document.data(bspec.value("visittime")._1)
 
      /* referrer */
      val referrer = document.data(bspec.value("referrer")._1)
      
      /* Format: (sessionid,timestamp,userid,pageurl,visittime,referrer) */
      (sessionid,timestamp,userid,pageurl,visittime,referrer)
      
    })
  
  }

}