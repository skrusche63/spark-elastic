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

/**
 * EnronApp is a helper to prepare and index data in ES
 */
object EnronApp {
  
  def main(args : Array[String]) {
    
    val settings = Map(
        
        "dir"     -> "/Work/tmp/enron/20110402/mails/allen-p",
        
        "index"   -> "enron",
        "mapping" -> "mails",
        
        "server"  -> "http://localhost:9200"
    
    )

    val action = "index" // or prepare
    EnronEngine.execute(action, settings)
    
  }
}
