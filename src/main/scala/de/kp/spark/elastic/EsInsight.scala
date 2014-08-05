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

import org.apache.spark.sql.SQLContext

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

object EsInsight {

  implicit val formats = Serialization.formats(NoTypeHints)

  def insight(sc:SparkContext, docs:RDD[(String,Map[String,String])]) {
    
    val sqlc = new SQLContext(sc)

    /**
     * Convert docs into JSON
     */
    val jdocs = docs.map(valu => {
      String.format("""{"id":"%s","doc":%s}""", valu._1, write(valu._2))
    })

    val table = sqlc.jsonRDD(jdocs)
    table.registerAsTable("docs")
  /**
   * Mixing SQL and other Spark operations
   */
    val subjects = sqlc.sql("SELECT doc.subject FROM docs").filter(row => row.getString(0).contains("Re"))    
    subjects.foreach(subject => println(subject))
    
  }
  
}