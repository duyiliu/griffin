package org.apache.griffin.measure.step.transform.profile

import org.apache.griffin.measure.step.transform.profile.operator.{DetailsOperatorParam, MinimumOperator, Operator, OperatorParam}
import org.apache.spark.sql.Row
import org.apache.spark.util.AccumulatorV2

import scala.collection.immutable.Map
import scala.collection.mutable

class OperatorAccumulator(private var tableProfileInfo: TableProfileInfo) extends AccumulatorV2[InputData, TableProfileInfo] {


  override def isZero: Boolean = {
    var result = true;
    tableProfileInfo.fieldProfileInfoMap.foreach(f => f._2.operators.foreach(
      v => if (!v._2.isZero()) result = false)
    )
    result
  }

  override def copy(): AccumulatorV2[InputData, TableProfileInfo] = {

    val operatorAccumulator = new OperatorAccumulator(tableProfileInfo)
    operatorAccumulator.tableProfileInfo = this.tableProfileInfo
    operatorAccumulator
  }

  override def reset(): Unit = {
    tableProfileInfo.clear()

    //tableProfileInfo = new TableProfileInfo()

    // tableProfileInfo
  }

  override def add(v: InputData): Unit = {

    val fieldProfileInfo = tableProfileInfo.getFieldProfileInfo(v.getColumnName).get;




    v.operators.foreach(f => {
      var operator: Operator = null;
      //var  operator=   fieldProfileInfo.operators.get(f._2.getOperatorType())
//      if (fieldProfileInfo.operators.get(f._2.getOperatorType()).isDefined) {
        operator = f._2;

//      } else {
//        operator = fieldProfileInfo.operators.get(f._2.getOperatorType()).get;
//      }
      operator.add(v.getValue);
      fieldProfileInfo.operators += (f._1 -> operator);

    })


  }

  override def merge(other: AccumulatorV2[InputData, TableProfileInfo]): Unit = {


    if (tableProfileInfo.fieldProfileInfoMap.size == 0) {
      this.tableProfileInfo = other.value;
    }

    other.value.fieldProfileInfoMap.foreach(f => {

      if (tableProfileInfo.fieldProfileInfoMap.get(f._1).isDefined) {
        tableProfileInfo.fieldProfileInfoMap.get(f._1).get.merge(f._2);
      }


    })

    other.value.clear()

  }

  override def value: TableProfileInfo = {
    tableProfileInfo
  }
}
