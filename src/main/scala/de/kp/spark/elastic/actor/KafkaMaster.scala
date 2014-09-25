package de.kp.spark.elastic.actor
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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import akka.routing.RoundRobinRouter

import de.kp.spark.elastic.{Configuration,EsConfig,EsEvents,EsTransportClient}

import scala.concurrent.duration._
import scala.concurrent.duration.Duration._

import scala.concurrent.duration.DurationInt

class KafkaMaster extends Actor with ActorLogging {
    
  private val (esHost,esPort,esIndex,esType) = Configuration.elastic  
  private val esClient = EsTransportClient(EsConfig(Seq(esHost),Seq(esPort.toInt)))
  
  private val esEvents = new EsEvents(esClient,esIndex,esType)
  
  import concurrent.ExecutionContext.Implicits._
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {

    case req:String => {
      
      val response = esEvents.insert(req)
      
    }
    
    case _ => {}
  
  }

}