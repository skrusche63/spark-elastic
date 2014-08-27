package de.kp.spark.cf
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
import org.apache.spark.SparkContext._

object EsNPref {

  def build(docs:RDD[(String,Map[String,String])],fields:Array[String]):RDD[(String,String,Int)] = {

    val transactions = docs.map(doc => {
      /**
       * Each document (doc) represents an ecommerce transaction per user
       */
      val user = doc._2(fields(0))
      val line = doc._2(fields(1))
      
      (user,line)
      
    })
    
    build(transactions)
  
  }
 
  def build(transactions:RDD[(String,String)]):RDD[(String,String,Int)] = {
    /**
     * STEP #1
     * 
     * Compute the total number of transactions per user. The transactions are
     * grouped by user (_._1) and then mapped onto number of transactions
     * per user
     */
    val total = transactions.groupBy(_._1).map(grouped => (grouped._1, grouped._2.size))    
    /**
     * STEP #2
     * 
     * Computer the item support per user. Each transaction (text line) is split
     * into an Array[String] and all items are made unique. The result is mapped
     * into (user,item,support) tuples
     */
    val userItemSupport = transactions.flatMap(valu => List.fromArray(valu._2.split(" ")).distinct.map(item => (valu._1,item)))
      .groupBy(valu => (valu._1,valu._2))
      .map(grouped => (grouped._1,grouped._2.size)).map(valu => (valu._1._1,valu._1._2,valu._2))   
    /**
     * STEP #3
     * 
     * Compute item preference per user. Item support and total transactions per user
     * are used to compute the respective item preference:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     */
    val userItemPref = userItemSupport.keyBy(value => value._1).join(total)
      .map(valu => {
        
        val user = valu._1        
        val data = valu._2 // ((user,item,support),total)
        
        val item = data._1._2
        val supp = data._1._3
        
        val total = data._2

        /**
         * Math.log means natural logarithm in Scala
         */
        val pref = Math.log(1 + supp.toDouble / total.toDouble)
        (user, item, pref)
        
      })
      
    /**
     * The user-item preferences are solely based on the purchase data of a 
     * particluar user; the respective value, however, is far from representing
     * a real-life value, as it only takes the purchase frequency into account.
     * 
     * The frequency is quite different depending on the item price, item lifetime, 
     * and the like. For example, since expensive items or items with long lifespan,
     * such as jewelry or electronic home appliances, are purchased infrequently.
     * 
     * So the preferences of users form them cannot be higher that cheap items or
     * those with a short lifespan such as hand creams or tissues. Also, when a 
     * user u purchase item i four times out of ten transactions, we may think that
     * he does not prefer item i if other users purchased the same item eight times
     * out of ten transactions.
     * 
     * It is therefore necessary to define a relative preference so it is comparable 
     * among all users. We therefore proceed to compute the maximum item preference
     * for all users and use this value to normalize the user-item preference derived
     * above.  
     */

    /**
     * STEP #4
     * 
     * Compute the maximum preference per item (independent of the user)
     */
    val itemMaxPref = userItemPref.map(valu => (valu._2,valu._3)).groupBy(valu => valu._1)
      .map(grouped => {
        
        def max(pref1:Double, pref2:Double):Double = if (pref1 > pref2) pref1 else pref2
        
        val item = grouped._1
        val mpref = grouped._2.map(valu => valu._2).reduceLeft(max)
        
        (item,mpref)

      })
    /**
     * STEP #5
     * 
     * Finally compute the user-item rating with scores from 1..5
     */

    val userItemRating = userItemPref.keyBy(valu => valu._2).join(itemMaxPref)
      .map(valu => {
        
        val item = valu._1
        val data = valu._2
        
        val uid = data._1._1
        val pref = data._1._3
        
        val mpref = data._2
        val npref = Math.round( 5* (pref.toDouble / mpref.toDouble) ).toInt
        
        (uid,item,npref)
        
      })
      
     userItemRating
     
  }
  
}