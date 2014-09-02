package de.kp.spark.elastic.samples
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

import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties

import org.apache.commons.io.Charsets

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

/**
 * Message refers to any Scala case class that is serializable or deserializable
 * with json4s
 */
class MessageDecoder(props: VerifiableProperties) extends Decoder[Message] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def fromBytes(bytes: Array[Byte]): Message = {
    read[Message](new String(bytes, Charsets.UTF_8))
  }

}

class MessageEncoder(props: VerifiableProperties) extends Encoder[Message] {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def toBytes(message: Message): Array[Byte] = {
    write[Message](message).getBytes(Charsets.UTF_8)
  }
  
}