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
 
import akka.actor.ActorSystem

import spray.http.{HttpRequest,HttpResponse}
import spray.client.pipelining.{Get,Post,sendReceive }
 
import scala.concurrent.Future
import scala.util.{Success,Failure}
/**
 * A Http client implementation based on Akka & Spray
 */
class EsClient {
  
  import concurrent.ExecutionContext.Implicits._
  
  implicit val system = ActorSystem("EsClient")
 
  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive

  def get(url:String):Future[HttpResponse] = pipeline(Get(url))

  def post(url:String,payload:String):Future[HttpResponse] = pipeline(Post(url, payload))
  
  def shutdown = system.shutdown
  
}