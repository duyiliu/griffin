package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.JsonProperty

class TopkCounter(@JsonProperty("i") var item: Any = null,
                  @JsonProperty("c")  var count: Long = 0L
                 ) extends Serializable {


}
