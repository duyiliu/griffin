package org.apache.griffin.measure.step.transform.profile.operator.result

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.griffin.measure.step.transform.profile.operator.AbnormalOperator

class AbnormalOperatorResult(@JsonIgnore recordsLimit:Int) extends Serializable {

  var count: Long = 0
  var records: Set[Any] = Set()



  def add(v: Any) = {
    count += 1
    if (records.size < recordsLimit) {
      records += v
    }

  }


  def merge(other: AbnormalOperatorResult) ={
    count+=other.count

    other.records.foreach(f=>{

      if (records.size < recordsLimit) {
        records += f
      }

    })




  }


}
