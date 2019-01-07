package org.apache.griffin.measure.step.transform.profile.operator

import org.apache.griffin.measure.Application.info
import org.apache.griffin.measure.step.transform.profile.operator.result.AbnormalOperatorResult

import scala.util.Success

case class AbnormalOperator(private val operatorParam: OperatorParam) extends Operator(operatorParam) {

  private val REGEX_CHECK_TYPE = "regex";
  private val JS_CHECK_TYPE = " ";


  private def getAbnormalOperatorResult = abnormalOperatorResult

  private val checkType = operatorParam.getConfig.getOrElse("check_type", REGEX_CHECK_TYPE).toString

  private val isAllowedNull = operatorParam.getConfig.getOrElse("is_allowed_null", true);

  private val recordsLimit: Int = operatorParam.getConfig.getOrElse("records_limit", "100").toString.toInt;


  private var abnormalOperatorResult: AbnormalOperatorResult = new AbnormalOperatorResult(recordsLimit)

  private val rule = operatorParam.getConfig.getOrElse("rule", "").toString


  override def add(v: Any): Unit = {

    if (v != null) {
      var checkResult = checkType match {

        case REGEX_CHECK_TYPE => regex(v)
        case JS_CHECK_TYPE => javascript(v)
        case _ => throw new Exception(s"check type ${checkType} is not supported!")
      }

      checkResult match {
        case true =>  abnormalOperatorResult.add(v)
        case false => None
      }


    } else {
      if (isAllowedNull == false) {
        abnormalOperatorResult.add(v);
      }
    }


  }

  private def regex(v: Any): Boolean = {

    if (rule.r.findFirstIn(v.toString).isEmpty) {
      true
    } else {
      false
    }


  }

  private def javascript(v: Any): Boolean = {

    true
  }

  override def merge(other: Operator): Unit = {

    if (abnormalOperatorResult != null) {

      abnormalOperatorResult.merge(other.asInstanceOf[AbnormalOperator].getAbnormalOperatorResult);

    } else {
      abnormalOperatorResult = other.asInstanceOf[AbnormalOperator].abnormalOperatorResult
    }

  }

  override def reset(): Unit = {
    abnormalOperatorResult = new AbnormalOperatorResult(recordsLimit)
  }

  override def value(): AbnormalOperatorResult = {
    abnormalOperatorResult
  }

  override def isZero(): Boolean = {
    abnormalOperatorResult.count == 0
  }
}

