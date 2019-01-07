package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import org.apache.griffin.measure.configuration.enums.{CardinalityOperatorType, OperatorType}

abstract  class Operator ( operatorParam: OperatorParam) extends  Serializable {


  def add(v: Any): scala.Unit

  def merge(other: Operator): scala.Unit
  def reset(): scala.Unit
  @JsonProperty("value")
  def value(): Any
  @JsonIgnore
  def  isZero(): Boolean

  @JsonProperty("operatorType")
  def getOperatorType(): String ={
    operatorParam.getOperatorType.desc
  }


}
