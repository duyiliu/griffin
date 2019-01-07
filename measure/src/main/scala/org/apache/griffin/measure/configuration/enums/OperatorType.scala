/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.configuration.enums



import scala.util.matching.Regex

/**
  * operator type
  */
sealed trait OperatorType {
  val idPattern: Regex
  val desc: String
}

object OperatorType {
  private val operatorTypes: List[OperatorType] = List(
    MinimumOperatorType,
    MaximumOperatorType,
    TopKOperatorType,
    CardinalityOperatorType,
    DataTypeDetectionOperatorType,
    AbnormalOperatorType,
    UnknownOperatorType
  )

  def apply(ptn: String): OperatorType = {
    operatorTypes.find(tp => ptn match {
      case tp.idPattern() => true
      case _ => false
    }).getOrElse(UnknownOperatorType)
  }

  def unapply(pt: OperatorType): Option[String] = Some(pt.desc)

  def validOperatorTypes(strs: Seq[String]): Seq[OperatorType] = {
    val seq = strs.map(s => OperatorType(s)).filter(_ != UnknownOperatorType).distinct
    //if (seq.size > 0) seq else Seq(ElasticsearchOperatorType)
    seq
  }
}

/**
  * minimum  operator
  */
case object MinimumOperatorType extends OperatorType {
  val idPattern = "^(?i)minimum|min$".r
  val desc = "Minimum"
}


/**
  * maximum  operator
  */
case object MaximumOperatorType extends OperatorType {
  val idPattern = "^(?i)maximum|max".r
  val desc = "Maximum"
}




/**
  * topk  operator
  */
case object TopKOperatorType extends OperatorType {
  val idPattern = "^(?i)topK$".r
  val desc = "TopK"
}


/**
  * Cardinality  operator
  */
  case object CardinalityOperatorType extends OperatorType {
  val idPattern = "^(?i)cardinality$".r
  val desc = "Cardinality"
}

/**
  * DataTypeDetection  operator
  */
case object DataTypeDetectionOperatorType extends OperatorType {
  val idPattern = "^(?i)DataTypeDetection$".r
  val desc = "DataTypeDetection"
}

/**
  * NullCount  operator
  */
case object NullCountOperatorType extends OperatorType {
  val idPattern = "^(?i)nullCount$".r
  val desc = "NullCount"
}

/**
  * Abnormal  operator
  */
case object AbnormalOperatorType extends OperatorType {
  val idPattern = "^(?i)Abnormal$".r
  val desc = "Abnormal"
}



case object UnknownOperatorType extends OperatorType {
  val idPattern = "".r
  val desc = "unknown"
}
