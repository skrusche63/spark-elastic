package de.kp.spark.elastic

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