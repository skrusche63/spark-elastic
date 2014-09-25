package de.kp.spark.elastic.specs
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

import scala.xml._
import scala.collection.mutable.HashMap

object PageViewSpec {

  private val spec = new FieldSpec("pageview.xml")
  private val fields = HashMap.empty[String,(String,String)]
  
  load()
  
  private def load() {

    for (field <- spec.root \ "field") {
      
      val _name  = (field \ "@name").toString
      val _type  = (field \ "@type").toString

      val _mapping = field.text
      fields += _name -> (_mapping,_type) 
      
    }

  }

  def get = fields.toMap
  
}