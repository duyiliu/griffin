package org.apache.griffin.measure.step.transform.profile

import com.clearspring.analytics.stream.StreamSummary
import org.apache.griffin.measure.step.transform.profile.operator.Operator
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class FieldProfileInfo(private var name :String)  extends  Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)



  /**
    * 列名
    */
  var fieldName = name

  /**
    * 算子列表
    */
  var operators: mutable.Map[String, Operator] = mutable.Map()


  def merge(other: FieldProfileInfo): FieldProfileInfo = {

    logger.debug(" FieldProfileInfomerge start："+fieldName)

    this.operators.foreach(f => {
      val operator = other.operators.get(f._1);
      if (operator.isDefined) {
        f._2.merge(other.operators.get(f._1).get);
      }else{
        logger.debug("")
      }

    })
    logger.debug(" FieldProfileInfo merge end："+fieldName)
     this


  }


}
