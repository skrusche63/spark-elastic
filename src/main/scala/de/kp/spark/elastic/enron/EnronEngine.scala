package de.kp.spark.elastic.enron
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
  
import java.io.File

import scala.io.Source
import scala.concurrent.Future

import spray.http.HttpResponse
import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.elastic.EsClient

/**
 * Please note, that part of the functionality below is taken from
 * the code base assigned to this blog entry:
 * 
 * http://sujitpal.blogspot.de/2012/11/indexing-into-elasticsearch-with-akka.html
 */

object EnronEngine {
  
  import concurrent.ExecutionContext.Implicits._
    
  private val client = new EsClient()

  private val shards:Int   = 1
  private val replicas:Int = 1
  
  private val es_CreateIndex:String = """
    {"settings": {"index": {"number_of_shards": %s, "number_of_replicas": %s}}}""".format(shards, replicas)
    
  private val es_CreateSchema:String = """{ "%s" : { "properties" : %s } }"""

  private val parser = new EnronParser()
  private val schema = new EnronSchema()

  def execute(action:String,settings:Map[String,String]) {
    
    action match {
      
      case "index" => 
        
        index(settings)
        client.shutdown
        
      case "prepare" =>
        
        prepare(settings)
        client.shutdown
        
      case _ => {}
      
    }
    
  }
  
  private def prepare(settings:Map[String,String]) {
    
    /**
     * Create new index
     */
    val server0 = List(settings("server"), settings("index")).foldRight("")(_ + "/" + _)
    client.post(server0, es_CreateIndex)
    
    /**
     * Create new schema
     */   
    val server1 = List(settings("server"), settings("index"), settings("mapping")).foldRight("")(_ + "/" + _)
    client.post(server1 + "_mapping", es_CreateSchema.format("enron", schema.mappings))

  }
  
  private def index(settings:Map[String,String]) {

    val dir = settings.get("dir").get
    
    val filefilter = new EnronFilter()
    val files = walk(new File(dir)).filter(f => filefilter.accept(f))

    val server1 = List(settings("server"), settings("index"), settings("mapping")).foldRight("")(_ + "/" + _)

    for (file <- files) {
      
      val path = file.getAbsolutePath()
      val doc = parser.parse(Source.fromFile(path))

      val response = addDocument(doc,server1)
      response.map(result => println("RESPONSE: " + result.entity.asString))

    }
  
  }
  
  private def getProps(path:String): Map[String,String] = {
    
    val file:File = new File(path)
    
    Map() ++ Source.fromFile(file).getLines().toList.
      filter(line => (! (line.isEmpty || line.startsWith("#")))).
      map(line => (line.split("=")(0) -> line.split("=")(1)))
  
  }  

  private def walk(root: File): Stream[File] = {
    
    if (root.isDirectory) {      
      root #:: root.listFiles.toStream.flatMap(walk(_))
    
    } else root #:: Stream.empty
  
  }

  private def addDocument(doc:EnronDoc, server:String):Future[HttpResponse] = {

    implicit val formats = Serialization.formats(NoTypeHints)
    val json = write(doc)
     
    client.post(server, json)

  }
 
}