package org.apache.griffin.measure.step.transform.profile

import java.io.Serializable
import java.util

import org.apache.griffin.measure.step.transform.profile.operator.Operator

import scala.collection.mutable


/**
  * @author chimney
  */
@SerialVersionUID(1L)
class InputData(

                 /**
                   * 列名
                   */
                 private var columnName: String, private var value: Any,var operators :mutable.Map[String, Operator]) extends Serializable {

  /**
    * 是否精确计算
    */
  private var accurate = false

  def isAccurate: Boolean = accurate

  def setAccurate(accurate: Boolean): Unit = {
    this.accurate = accurate
  }

  def getValue: Any = value


  def setValue(value: String): Unit = {
    this.value = value
  }


  def getColumnName: String = columnName

  def setColumnName(columnName: String): Unit = {
    this.columnName = columnName
  }


  def getOperator :mutable.Map[String, Operator]  =  operators

}
