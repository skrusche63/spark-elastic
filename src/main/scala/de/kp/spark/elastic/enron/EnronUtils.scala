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

import scala.io.Source
import scala.collection.immutable.HashMap

import java.io.{File,FileFilter}
import java.util.Locale

import java.text.SimpleDateFormat

/**
 * Please note, that part of the functionality below is taken from
 * the code base assigned to this blog entry:
 * 
 * http://sujitpal.blogspot.de/2012/11/indexing-into-elasticsearch-with-akka.html
 */

case class EnronDoc (    
    message_id: String,
    from: String,
    to: Seq[String],
    x_cc: Seq[String],
    x_bcc: Seq[String],
    date: String,
    subject: String,
    body:String
)

class EnronSchema {
  
  def mappings(): String = """{
    "message_id": {"type": "string", "index": "not_analyzed", "store": "yes"},
    "from": {"type": "string", "index": "not_analyzed", "store": "yes"},
    "to": {"type": "string", "index": "not_analyzed", "store": "yes", "multi_field": "yes"},
    "x_cc": {"type": "string", "index": "not_analyzed", "store": "yes", "multi_field": "yes"},
    "x_bcc": {"type": "string", "index": "not_analyzed", "store": "yes", "multi_field": "yes"},
    "date": {"type": "date", "index": "not_analyzed", "store": "yes"},
    "subject": {"type": "string", "index": "analyzed", "store": "yes"},
    "body": {"type": "string", "index": "analyzed", "store": "yes"}
  }"""
    
}

class EnronParser {

  def parse(source: Source):EnronDoc = {
    
    val map = parse(source.getLines(), HashMap[String,String](), false)
    /**
     * Convert map into case class
     */
    val message_id = map.get("message_id").get
    val from = map.get("from").get
    
    val to = map.get("to") match {
      case None => Seq()
      case Some(to) => to.split(",").toSeq
    }
    
    val x_cc = map.get("x_cc").get.split(",").toSeq

    val x_bcc = map.get("x_bcc").get.split(",").toSeq
    val date = map.get("date").get

    val subject = map.get("subject").get
    val body = map.get("body").get
    
    new EnronDoc(message_id,from,to,x_cc,x_bcc,date,subject,body)

  }
  
  private def parse(lines: Iterator[String], map: Map[String,String], startBody: Boolean): Map[String,String] = {
    
    if (lines.isEmpty) map
    else {
      
      val head = lines.next()
      
      if (head.trim.length == 0) parse(lines, map, true)
      else if (startBody) {
      
        val body = map.getOrElse("body", "") + "\n" + head
        parse(lines, map + ("body" -> body), startBody)
      
      } else {
        
        val split = head.indexOf(':')
        if (split > 0) {
          val kv = (head.substring(0, split), head.substring(split + 1))
          val key = kv._1.map(c => if (c == '-') '_' else c).trim.toLowerCase
          val value = kv._1 match {
            case "Date" => formatDate(kv._2.trim)
            case _ => kv._2.trim
          }
          parse(lines, map + (key -> value), startBody)
        } else parse(lines, map, startBody)
      }
    }
  }
  
  private def formatDate(date: String): String = {

    lazy val parser = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss", Locale.US)
    lazy val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    
    formatter.format(parser.parse(date.substring(0, date.lastIndexOf('-') - 1)))
  
  }

}
/**
 * We restrict to the /sent/ folders of the Enron dataset
 */
class EnronFilter extends FileFilter {
  
  override def accept(file: File): Boolean = {
    file.getAbsolutePath().contains("/sent/")
  }
  
}