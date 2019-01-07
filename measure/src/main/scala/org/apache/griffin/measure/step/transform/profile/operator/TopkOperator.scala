package org.apache.griffin.measure.step.transform.profile.operator

import java.util.stream.{Collector, Collectors}

import com.clearspring.analytics.stream.{Counter, StreamSummary}
import com.fasterxml.jackson.annotation.JsonProperty
import com.saicmotor.datagovern.profile.global.CustomDate
import com.saicmotor.datagovern.profile.utils.DataFormatUtils
import org.apache.griffin.measure.configuration.enums.OperatorType
import scala.collection.JavaConversions._

/**
  * TopK算子
  * @param operatorParam
  */
case class TopkOperator(private val operatorParam: OperatorParam) extends Operator(operatorParam) {

  private val topKCapacity: Int = 1000;
  private val showCapacity: Int = 100;


  private var streamSummary: StreamSummary[Any] = init;

  private def init: StreamSummary[Any] = {
    var streamSummary: StreamSummary[Any] = new StreamSummary[Any](topKCapacity);
    streamSummary
  }

  override def add(o: Any): Unit = {

    streamSummary.offer(o);
  }


  override def value(): List[TopkCounter] = {
    if (streamSummary != null) {

      var topKList: List[TopkCounter] = List();

      for (f <- streamSummary.topK(showCapacity)) {

        topKList = topKList :+ new TopkCounter(f.getItem, (f.getCount - f.getError));

      }

      topKList
    } else {
      null
    }

  }

  override def merge(other: Operator): Unit = {


    other.asInstanceOf[TopkOperator].value().foreach(f => {

      streamSummary.offer(f.item, f.count.toInt);

    })

  }

  override def reset(): Unit = {
    streamSummary = init;

  }


  override def isZero(): Boolean = {
    if (streamSummary.size() == 0) {
      true
    } else {
      false
    }
  }

}
