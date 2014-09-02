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

import scala.collection.mutable.{ArrayBuffer,HashMap}
import scala.util.control.Breaks._

object GoalSpec extends Serializable {
 
  private val spec = new FieldSpec("goals.xml")
  private val flows = HashMap.empty[String,Array[String]]
  
  val FLOW_NOT_ENTERED:Int = 0
  val FLOW_ENTERED:Int     = 1
  val FLOW_COMPLETED:Int   = 2
  
  load()
  
  private def load() {

    for (goal <- spec.root \ "goal") {
      
      val fid  = (goal \ "@id").toString
      val flow = goal.text.split(",")
      
      flows += fid -> flow
      
    }

  }

  def getFlow(fid:String):Option[Array[String]] = {
    flows.get(fid)
  }
  
  def getFlows():Array[(String,Array[String])] = {
    flows.toArray
  }
  
  def checkFlow(goal:String,pages:List[String]):Int = {
    
    getFlow(goal) match {
  
      case None => 0
      case Some(flow) => checkFlow(flow,pages)
    
    }
  
  }
  
  /**
   * A helper method to evaluate whether the pages clicked in a certain 
   * session match, partially match or do not match a predefined sequence
   * of pages (flow)
   */
  def checkFlow(flow:Array[String],pages:List[String]):Int = { 			
    		
    var j = 0
    var	flowStat = FLOW_NOT_ENTERED
    		
    var matched = false;
    		
    for (i <- 0 until flow.length) {
    			
      breakable {while (j < pages.size) {
    				
        matched = false
        /*
         * We expect that a certain page url has to start with the 
         * configured url part of the flow
         */
    	if (pages(j).startsWith(flow(i))) {
    	  flowStat = (if (i == flow.length - 1) FLOW_COMPLETED else FLOW_ENTERED)
    	  matched = true
    				
    	}
    	j += 1
    	if (matched) break
    			
      }}
    
    }

    flowStat
    
  }
  
  /**
   * A helper method to evaluate whether the pages clicked in a certain 
   * session match, partially match or do not match a predefined sequences
   * of page flows
   */
  def checkFlows(pages:List[String]):Array[(String,Int)] = { 			
    
    val flows = getFlows
    flows.map(v => (v._1, checkFlow(v._2,pages)))
    
  }
 
}