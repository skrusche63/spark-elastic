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

import org.elasticsearch.client.Client

import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.ActionListener

import scala.concurrent.{ExecutionContext,Future,Promise}

/**
 * EsEvents indexes trackable events retrieved from Apache Kafka
 */
class EsEvents(client:Client,index:String,mapping:String) {

  def insert(event:String)(implicit ec:ExecutionContext): Future[Either[String,String]] = {
    
    val response = Promise[IndexResponse]

    /* index/mapping = enron/mails */
    client.prepareIndex(index,mapping).setSource(event)
      .execute(new EsActionListener(response))

    response.future
      .map(r => Right(r.getId()))
      .recover {
        case e: Exception => Left(e.toString)
      }
  
  }

}

class EsActionListener[T](val p: Promise[T]) extends ActionListener[T]{
  
  override def onResponse(r: T) = {
    p.success(r)
  }
  
  override def onFailure(e: Throwable) = {
    p.failure(e)
  }

}