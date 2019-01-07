package org.apache.griffin.measure.step.transform.profile

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.mutable

class TableProfileInfo extends  Serializable {

  /**
    * 行数
    */
  var rowCount:Long=0L;
  /**
    * 重复数量
    */
  var repeatCount:Long=0L;


  @JsonProperty("fieldProfileInfos")
  var fieldProfileInfoMap:  mutable.Map[String, FieldProfileInfo] = mutable.Map();


  def getFieldProfileInfo(field: String): Option[FieldProfileInfo] = {

    if (!fieldProfileInfoMap.contains(field)) {
      fieldProfileInfoMap += (field -> new FieldProfileInfo(field));
    }

    fieldProfileInfoMap.get(field)


  }


  def clear(): Unit = {

    for (pi <- this.fieldProfileInfoMap.values) {
      pi.operators.foreach(f=>f._2.reset())
    }
     // this.fieldProfileInfoMap.clear
  }
}
