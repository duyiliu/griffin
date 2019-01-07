package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.JsonProperty
import com.saicmotor.datagovern.profile.global.CustomDate
import com.saicmotor.datagovern.profile.utils.DataFormatUtils
import org.apache.griffin.measure.configuration.enums.OperatorType

/**
  * 最大值算子
  * @param operatorParam
  */
case class MaximumOperator(private val operatorParam: OperatorParam) extends Operator(operatorParam) {

 private var maximumValue: Any = null;

  override def add(o: Any): Unit = {

    maximumValue = compare(maximumValue, o);
  }


  override def value(): Any = {
    maximumValue
  }

  override def merge(other: Operator): Unit = {
    maximumValue = compare(maximumValue, other.value());


  }

  override def reset(): Unit = {
    maximumValue = null;
  }

  private def compare(maximumValue: Any, o: Any): Any = {
    if (maximumValue == null && o != null) {
      o

    } else if (maximumValue == null || o == null) {

      maximumValue

    } else {


      val c = DataFormatUtils.offer(o.toString);


      if (c.isInstanceOf[CustomDate]) {

        if (c.asInstanceOf[CustomDate].getValue.compareTo(maximumValue.asInstanceOf[CustomDate].getValue) > 0) {
          o
        } else {
          maximumValue
        }

      } else if (c.isInstanceOf[java.math.BigDecimal]) {

        if ( BigDecimal.apply(c.toString).compare( BigDecimal.apply(maximumValue.toString)) > 0) {
          o
        } else {
          maximumValue
        }

      } else {

        if (c.toString.length.compareTo(maximumValue.toString.length) > 0) {
          o
        } else {
          maximumValue
        }


      }
    }
  }

  override def isZero(): Boolean ={
    if(maximumValue==null){
      true
    }else{
      false
    }
  }
}
