package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import org.apache.griffin.measure.configuration.dqdefinition.Param

@JsonInclude(Include.NON_NULL)
case class DetailsOperatorParam(@JsonProperty("datasource") val datasource: String,
                                @JsonProperty("field_operators") val fieldOperators: Map[String, List[OperatorParam]]

                               ) extends Param {


  def validate(): Unit = {

  }
}
