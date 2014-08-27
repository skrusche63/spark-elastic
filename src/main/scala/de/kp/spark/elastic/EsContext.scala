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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{SchemaRDD,SQLContext}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ArrayWritable,MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsInputFormat

import org.json4s.native.Serialization.write
import org.json4s.DefaultFormats

import scala.collection.JavaConversions._

case class EsDocument(id:String,data:Map[String,String])

/**
 * ElasticContext supports access to Elasticsearch from Apache Spark using the library
 * from org.elasticsearch.hadoop. For read requests, the [Text] specifies the _id field
 * from Elasticsearch, and [MapWritable] specifies a (field,value) map
 * 
 */
class ElasticContext(sparkConf:Configuration) extends SparkBase {
  
  private val sc = createCtxLocal("ElasticContext",sparkConf)
  private val sqlc = new SQLContext(sc)

  /**
   * EsDocument is the common format to be used if machine learning algorithms
   * have to be applied to the extracted content of an Elasticseach index
   */
  def documents(esConf:Configuration):RDD[EsDocument] = {
    
    val source = sc.newAPIHadoopRDD(esConf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    source.map(hit => new EsDocument(hit._1.toString,toMap(hit._2)))
    
  }
  /**
   * Json format is the common format to be used if SQL queries have to be applied
   * to the extracted content of an Elasticsearch index
   */
  def documentsAsJson(esConf:Configuration):RDD[String] = {
    
    implicit val formats = DefaultFormats    
    
    val source = sc.newAPIHadoopRDD(esConf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val docs = source.map(hit => {
      val doc = Map("ident" -> hit._1.toString()) ++ toMap(hit._2)
      write(doc)      
    })
    
    docs
    
  }

  /**
   * Cluster extracted content from an Elasticsearch index by applying KMeans 
   * clustering algorithm from MLLib
   */
  def cluster(documents:RDD[EsDocument],esConf:Configuration):RDD[(Int,EsDocument)] =  {
    
    val fields = esConf.get("es.fields").split(",")
    val vectors = documents.map(doc => toVector(doc.data,fields))   

    val clusters = esConf.get("es.clusters").toInt
    val iterations = esConf.get("es.iterations").toInt
    
    /* Train model */
    val model = KMeans.train(vectors, clusters, iterations)
    
    /* Apply model */
    documents.map(doc => (model.predict(toVector(doc.data,fields)),doc))
    
  }

  /**
   * Apply SQL statement to extracted content from an Elasticsearch index
   */
  def query(documents:RDD[String], esConfig:Configuration):SchemaRDD =  {

    val query = esConfig.get("es.sql")
    val name  = esConfig.get("es.table")
    
    val table = sqlc.jsonRDD(documents)
    table.registerAsTable(name)

    sqlc.sql(query)   

  }

  /**
   * Wrapper to stop SparkContext
   */
  def shutdown = sc.stop
  /**
   * Wrapper to get SparkContext from ElasticContext
   */
  def sparkContext = sc
  
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

  private def toVector(data:Map[String,String], fields:Array[String]):Vector = {
    
    val features = data.filter(kv => fields.contains(kv._1)).map(_._2.toDouble)      
    Vectors.dense(features.toArray)
   
  }

}