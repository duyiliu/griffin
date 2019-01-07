package org.apache.griffin.measure.step.transform.profile.operator

import com.fasterxml.jackson.annotation.JsonProperty
import com.saicmotor.datagovern.profile.global.CustomDate
import com.saicmotor.datagovern.profile.utils.DataFormatUtils
import org.apache.griffin.measure.configuration.enums.OperatorType
import org.apache.griffin.measure.step.transform.profile.operator.result.DataTypeDetectionResult

case class DataTypeDetectionOperator(private val operatorParam: OperatorParam) extends Operator(operatorParam) {

  private var dataTypeDetectionResult: DataTypeDetectionResult = new DataTypeDetectionResult();

  override def add(v: Any): Unit = {

    if (v == null) {
      dataTypeDetectionResult.nullCount += 1
    } else {


      val c = DataFormatUtils.offer(v.toString);


      if (c.isInstanceOf[CustomDate]) {

        dataTypeDetectionResult.dateCount += 1

      } else if (c.isInstanceOf[java.math.BigDecimal]) {

        dataTypeDetectionResult.numericCount += 1

      } else {
        dataTypeDetectionResult.stringCount + 1

      }

    }
  }

  override def merge(other: Operator): Unit = {

    this.dataTypeDetectionResult.nullCount += other.asInstanceOf[DataTypeDetectionOperator].value().nullCount
    this.dataTypeDetectionResult.dateCount += other.asInstanceOf[DataTypeDetectionOperator].value().dateCount
    this.dataTypeDetectionResult.stringCount += other.asInstanceOf[DataTypeDetectionOperator].value().stringCount
    this.dataTypeDetectionResult.numericCount += other.asInstanceOf[DataTypeDetectionOperator].value().numericCount

  }

  override def reset(): Unit = {
    dataTypeDetectionResult=new DataTypeDetectionResult()
  }

  override def value(): DataTypeDetectionResult = {
    dataTypeDetectionResult
  }

  override def isZero(): Boolean = {
    if(dataTypeDetectionResult.numericCount ==0  && dataTypeDetectionResult.nullCount==0 && dataTypeDetectionResult.stringCount==0 && dataTypeDetectionResult.dateCount==0){
      true
    }else{
      false
    }
  }
}
