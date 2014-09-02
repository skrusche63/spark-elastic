package de.kp.spark.elastic.bayes
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import de.kp.spark.elastic.specs.GoalSpec

class ClickModel(probabilities:Map[Int,Double]) {
  
  def predict(clicks:Int):Double = {
    
    val nearest = probabilities.map(valu => {
      
      val k = valu._1
      val d = Math.abs(k - clicks)

      (k,d)
      
    }).toList.sortBy(_._2).take(1)(0)._1
   
    probabilities(nearest)
    
  }
}

/**
 * This Predictor is backed by the Bayesian Discriminant method 
 * to determine the conversion probability given a number of clicks 
 * within a certain web session; in this context, a web session is 
 * considered to be converted, if a certain sequence of page views 
 * appeared
 */
object ClickTrainer {
  
  /**
   * Input = (sessid,userid,total,starttime,timespent,referrer,exiturl,flowstatus)
   */
  def train(dataset:RDD[(String,String,Int,Long,Long,String,String,Int)]):ClickModel = {
    
    val histo = histogram(dataset)
    
    /*
     * p(c|v=1): probability of clicks per session, given the visitor converted in the session
     */
    val prob1 = histo.filter(valu => {valu._1._2 == 1}).map(valu => {
      
      val (clicks,converted) = valu._1
      val support = valu._2
      
      val prop = 1.toDouble / support
      (clicks,prop)
      
    }).collect().toMap
    
    /*
     * (p(c|v=0): probability of clicks per session, given the visitor did not convert in the session
     */
    val prob2 = histo.filter(valu => {valu._1._2 == 0}).map(valu => {
      
      val (clicks,converted) = valu._1
      val support = valu._2
      
      val prop = 1.toDouble / support
      (clicks,prop)
      
    }).collect().toMap
    
    val counts = conversions(dataset)
    
    /*
     * p(v=1): unconditional probability of visitor converted in a session
     */
    val prob3 = 1.toDouble / counts.filter(valu => valu._1 == 1).map(valu => valu._2).collect()(0)
    
    /*
     * p(v=0): unconditional probability of visitor did not convert in a session
     */
    val prob4 = 1.toDouble / counts.filter(valu => valu._1 == 0).map(valu => valu._2).collect()(0)

    /*
     * p(v=1|c) = p(c|v=1) * p(v=1) / (p(c|v=0) * p(v=0) + p(c|v=1) * p(v=1))
     */
    val clickProbs = prob1.map(valu => {
      
      val (clicks,prop) = valu
      
      val numerator = prop * prob3
      val denominator = numerator + prob4 * prob2(clicks)
      
      val res = (if (denominator > 0) numerator / denominator else 0)
      
      (clicks,res)
      
    })
    
    new ClickModel(clickProbs)
    
  }

  /**
   * Input = (sessid,userid,total,starttime,timespent,referrer,exiturl,flowstatus)
   * 
   */
  private def conversions(dataset:RDD[(String,String,Int,Long,Long,String,String,Int)]):RDD[(Int,Int)] = {
    
    val counts = dataset.map(valu => {
      
      val userConvertedPerSession = if (valu._8 == GoalSpec.FLOW_COMPLETED) 1 else 0
      
      val k = userConvertedPerSession
      val v = 1
      
      (k,v)
       
    }).reduceByKey(_ + _)
  
    /* 
     * The output shows the session counts 
     * for conversion and no conversion
     */
    counts
    
  }

  /**
   * Input = (sessid,userid,total,starttime,timespent,referrer,exiturl,flowstatus)
   * 
   */
  private def histogram(dataset:RDD[(String,String,Int,Long,Long,String,String,Int)]):RDD[((Int,Int),Int)] = {
    /*
     * The input contains one row per session. Each row contains the number of clicks 
     * in the session, time spent in the session and a boolean indicating whether the 
     * user converted during the session.
     */
    val histogram = dataset.map(valu => {
      
      val clicksPerSession = valu._3
      val userConvertedPerSession = if (valu._8 == GoalSpec.FLOW_COMPLETED) 1 else 0
      
      val k = (clicksPerSession,userConvertedPerSession)
      val v = 1
      
      (k,v)
    
    }).reduceByKey(_ + _)
    
    /*
     * Each row of the output contains the conversion flag, click count 
     * per session and the number of sessions with those click counts. 
     */ 
    histogram
    
  }

}