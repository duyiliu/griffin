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
package org.apache.griffin.measure.step.transform.profile.operator

import scala.util.{Success, Try}
import org.apache.griffin.measure.configuration.dqdefinition.SinkParam
import org.apache.griffin.measure.configuration.enums._
import org.apache.griffin.measure.sink.MultiSinks

import scala.collection.mutable


object OperatorFactory {

  /**
    * 创建算子
    *
    * @return
    */
  def getOperators(operatorParams: Iterable[OperatorParam]): mutable.Map[String, Operator] = {
    var operatorMap: mutable.Map[String, Operator] = mutable.Map();
    operatorParams.flatMap(param => getOperator(param)).foreach(f => {
      operatorMap += (f.getOperatorType() -> f);
    })

    operatorMap
  }

  private def getOperator(operatorParam: OperatorParam): Option[Operator] = {

    val operatorType = operatorParam.getOperatorType
    val operatorTry = operatorType match {
      case MinimumOperatorType => Try(MinimumOperator(operatorParam))
      case MaximumOperatorType => Try(MaximumOperator(operatorParam))
      case TopKOperatorType => Try(TopkOperator(operatorParam))
      case CardinalityOperatorType => Try(CardinalityOperator(operatorParam))
      case DataTypeDetectionOperatorType => Try(DataTypeDetectionOperator(operatorParam))
      case AbnormalOperatorType => Try(AbnormalOperator(operatorParam))

      case _ => throw new Exception(s"sink type ${operatorType} is not supported!")
    }
    operatorTry match {
      case Success(operatorTry) => Some(operatorTry)
      case _ => None
    }
  }

}
