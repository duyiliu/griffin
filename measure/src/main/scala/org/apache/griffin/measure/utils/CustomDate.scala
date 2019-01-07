package com.saicmotor.datagovern.profile.global



/**
  * @author chimney
  */
 class CustomDate extends Comparable[CustomDate] with Serializable {
  private var value:String = null

  def getValue: String = value

  def setValue(value: String): Unit = {
    this.value = value
  }

  override def compareTo(o: CustomDate): Int = this.value.compareTo(o.getValue)
}
