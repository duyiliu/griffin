package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.griffin.measure.configuration.dqdefinition.Param
import org.apache.griffin.measure.configuration.enums.{MinimumOperatorType, OperatorType}


class OperatorParam(@JsonProperty("name") private val name: String,
                    @JsonProperty("config") private val config: Map[String,Any] = null
                   ) extends Param {


  def getName: String = if (name != null) name else ""
  def getOperatorType: OperatorType = OperatorType(name)
  def getConfig:  Map[String,Any] = if (config != null) config else Map[String, Any]()

  def validate(): Unit = {

  }



}
