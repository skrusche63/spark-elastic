package de.kp.spark.elastic.ml
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

class EsSimilarity(ratings:RDD[(String,String,Int)]) {
  /**
   * Parameters to regularize correlation.
   */
  val PRIOR_COUNT = 10
  val PRIOR_CORRELATION = 0

  val model = build()
  
  def recommend(item:String, k:Int):Array[(String,Double,Double,Double,Double)] = {
    
    /**
     * Retrieve all similarities where the first item
     * is equal to the provided one
     */
    val similarities = model.filter(valu => {
      
      val pair = valu._1
      pair._1 == item
      
    })

    val result = similarities.map(valu => {
      
      val (item1,item2) = valu._1
      val (corr,rcorr,cos,jac) = valu._2
      
      (item2, corr, rcorr, cos, jac)

    }).collect().filter(valu => (valu._3 equals Double.NaN) == false)
    .sortBy(valu => valu._4).take(k)
    
    result
    
  }
  
  private def build():RDD[((String,String),(Double,Double,Double,Double))] = {
    
    /**
     * Compute the number of raters per item
     */ 
    val itemSupport = ratings.groupBy(valu => valu._2)
      .map(grouped => (grouped._1, grouped._2.size))
    /**
     * Join rating with item support: the result contains
     * the following data (user,item,rating,support)
     */
    val ratingsSupport = ratings.groupBy(valu => valu._2).join(itemSupport)
    .flatMap(joined => joined._2._1.map(valu => (valu._1, valu._2, valu._3, joined._2._2)))
    /**
     * Clone data, join on user and filter pairs to make sure
     * that we do not double count and exclude self pairs 
     */
    val ratingsSupportClone = ratingsSupport.keyBy(valu => valu._1)
    val ratingsPairs = ratingsSupportClone.join(ratingsSupportClone).filter(valu => valu._2._1._2 < valu._2._2._2)

    /** 
     * Compute raw inputs to similarity metrics
     */
    val vectorCalcs = ratingsPairs.map(valu => {
      
      val (user1,item1,rating1,support1) = valu._2._1
      val (user2,item2,rating2,support2) = valu._2._2
      
      val key = (item1, item2)
      val stats = (
        rating1 * rating2,
        rating1,                
        rating2,                
        math.pow(rating1, 2),   
        math.pow(rating2, 2),   
        support1,  
        support2
      )                
      
      (key, stats)
    
    }).groupByKey().map(valu => {
        
      val key   = valu._1
      val stats = valu._2
      
      val size = stats.size
      val dotProduct = stats.map(f => f._1).sum
      
      val rating1Sum = stats.map(f => f._2).sum
      val rating2Sum = stats.map(f => f._3).sum
      
      val rating1Sq = stats.map(f => f._4).sum
      val rating2Sq = stats.map(f => f._5).sum
      
      val support1 = stats.map(f => f._6).max
      val support2 = stats.map(f => f._7).max
        
      (key, (size, dotProduct, rating1Sum, rating2Sum, rating1Sq, rating2Sq, support1, support2))
    
    })
    
    /** 
     * Compute similarity metrics for each item pair
     */
    vectorCalcs.map(valu => {
        
      val key = valu._1
      val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, support, support2) = valu._2
      /*
       * Correlation
       */
      val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
      val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
      /*
       *  Cosine similarity
       */  
      val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
      /*
       * Jaccard Similarity
       */
      val jaccard = jaccardSimilarity(size, support, support2)

      (key, (corr, regCorr, cosSim, jaccard))
      
    })
    
  }

  /**
   * The correlation between two vectors A, B is cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to:
   * 
   * [n * dotProduct(A, B) - sum(A) * sum(B)] / sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  private def correlation (
      size:Double,
      dotProduct:Double,
      rating1Sum:Double,
      rating2Sum:Double,
      rating1NormSq:Double,
      rating2NormSq:Double) = {

    val numerator = size * dotProduct - rating1Sum * rating2Sum
    val denominator = scala.math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) * scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
    
  }

  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   *   
   * RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  private def regularizedCorrelation (
      size:Double,
      dotProduct:Double,
      rating1Sum:Double,
      rating2Sum:Double,
      rating1NormSq:Double,
      rating2NormSq:Double,
      virtualCount:Double,
      priorCorrelation:Double) = {

    
    val unregularizedCorrelation = correlation(size,dotProduct,rating1Sum,rating2Sum,rating1NormSq,rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  
  }

  /**
   * The cosine similarity between two vectors A, B is dotProduct(A, B) / (norm(A) * norm(B))
   */
  private def cosineSimilarity (
      dotProduct:Double, 
      rating1Norm:Double,
      rating2Norm:Double) = {
    
    dotProduct / (rating1Norm * rating2Norm)
  
  }

  /**
   * The Jaccard Similarity between two sets A, B is |Intersection(A, B)| / |Union(A, B)|
   */
  private def jaccardSimilarity (
      usersInCommon:Double, 
      totalUsers1:Double, 
      totalUsers2:Double) = {
    
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  
  }
  
}