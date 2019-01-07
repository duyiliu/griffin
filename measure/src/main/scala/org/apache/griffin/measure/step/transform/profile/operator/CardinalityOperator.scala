package org.apache.griffin.measure.step.transform.profile.operator

import com.clearspring.analytics.stream.StreamSummary
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.griffin.measure.configuration.enums.OperatorType

import scala.collection.JavaConversions._
import com.clearspring.analytics.stream.cardinality.CountThenEstimate
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.clearspring.analytics.stream.cardinality.ICardinality
import com.clearspring.analytics.util.IBuilder
import com.clearspring.analytics.stream.cardinality.ICardinality

case class CardinalityOperator(private val  operatorParam: OperatorParam) extends Operator(operatorParam) {



  private var cardinalityCalculator: ICardinality = init()

  private val CARDINALITY_PRECISION = 12
  private val CARDINALITY_SPARCE_PRECISION = 25
  private val CARDINALITY_TIPPING_POINT = 2000

  private def getCardinalityCalculator: ICardinality = cardinalityCalculator;

  override def add(o: Any): Unit = {


    cardinalityCalculator.offer(o);
  }


  override def value(): Long = {
    if (cardinalityCalculator != null) {

      println("-------------:"+   cardinalityCalculator.cardinality())

      cardinalityCalculator.cardinality()

    } else {
      0
    }

  }

  override def merge(other: Operator): Unit = {

    if (cardinalityCalculator != null) {
      cardinalityCalculator =  cardinalityCalculator.merge(other.asInstanceOf[CardinalityOperator].getCardinalityCalculator)
    } else {
      cardinalityCalculator = other.asInstanceOf[CardinalityOperator].getCardinalityCalculator
    }


  }

  override def reset(): Unit = {
    cardinalityCalculator = init();
  }

  private def init(): ICardinality = {

    val builder = new HyperLogLogPlus.Builder(CARDINALITY_PRECISION, CARDINALITY_SPARCE_PRECISION)

    val card = new CountThenEstimate(CARDINALITY_TIPPING_POINT, builder)

    card
  }


  override def isZero(): Boolean = {
    if (cardinalityCalculator.cardinality() == 0) {
      true
    } else {
      false
    }
  }
}
