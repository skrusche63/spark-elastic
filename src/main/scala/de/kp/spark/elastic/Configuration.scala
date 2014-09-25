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
import com.typesafe.config.ConfigFactory
import java.util.Properties

object Configuration {

    /* Load configuration for router */
  val path = "application.conf"
  val config = ConfigFactory.load(path)

  def elastic():(String,String,String,String) = {
  
    val cfg = config.getConfig("elastic")

    val host = cfg.getString("host")
    val port = cfg.getString("port")

    val index = cfg.getString("index")
    val mapping = cfg.getString("mapping")

    (host,port,index,mapping)    
  
  }

  def kafka():Properties = {
    
    val cfg = config.getConfig("kafka")

    val host = cfg.getString("zk.connect.host")
    val port = cfg.getString("zk.connect.port")
    
    val gid = config.getString("consumer.groupid")

    val ctimeout = cfg.getString("consumer.timeout.ms")
    val stimeout = cfg.getString("consumer.socket.timeout.ms")

    val ccommit = cfg.getString("consumer.commit.ms")
    val aoffset = cfg.getString("auto.offset.reset")
    
    val params = Map(
      "zookeeper.connect" -> (host + ":" + port),

      "group.id" -> gid,

      "socket.timeout.ms" -> stimeout,
      "consumer.timeout.ms" -> ctimeout,

      "autocommit.interval.ms" -> ccommit,
      "auto.offset.reset" -> aoffset
    
    )

    val props = new Properties()
    params.map(kv => {
      props.put(kv._1,kv._2)
    })

    props

  }
  
  def router():(Int,Int,Int) = {
  
    val cfg = config.getConfig("router")
  
    val time    = cfg.getInt("time")
    val retries = cfg.getInt("retries")  
    val workers = cfg.getInt("workers")
    
    (time,retries,workers)

  }

  def topic() = config.getString("topic")
}