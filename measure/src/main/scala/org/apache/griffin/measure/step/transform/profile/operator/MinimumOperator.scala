package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.saicmotor.datagovern.profile.global.CustomDate
import com.saicmotor.datagovern.profile.utils.DataFormatUtils
import org.apache.griffin.measure.configuration.enums.OperatorType

/**
  * 最小值算子
  * @param operatorParam
  */
case class MinimumOperator(private val operatorParam: OperatorParam) extends Operator(operatorParam) {



 private var minimumValue: Any = null;

  override def add(o: Any): Unit = {

    minimumValue = compare(minimumValue, o);
  }


  override def value(): Any = {
    minimumValue
  }

  override def merge(other: Operator): Unit = {
    minimumValue = compare(minimumValue, other.value());


  }

  override def reset(): Unit = {
    minimumValue = null;
  }

  private def compare(minimumValue: Any, o: Any): Any = {
    if (minimumValue == null && o != null) {
      o

    } else if (minimumValue == null || o == null) {

      minimumValue

    } else {


      val c = DataFormatUtils.offer(o.toString);


      if (c.isInstanceOf[CustomDate]) {

        if (c.asInstanceOf[CustomDate].getValue.compareTo(minimumValue.asInstanceOf[CustomDate].getValue) < 0) {
          o
        } else {
          minimumValue
        }

      } else if (c.isInstanceOf[java.math.BigDecimal]) {

        if ( BigDecimal.apply(c.toString).compare( BigDecimal.apply(minimumValue.toString)) < 0) {
          o
        } else {
          minimumValue
        }

      } else {

        if (c.toString.length.compareTo(minimumValue.toString.length) < 0) {
          o
        } else {
          minimumValue
        }


      }
    }
  }
  override def isZero(): Boolean ={
    if(minimumValue==null){
      true
    }else{
      false
    }
  }

}
