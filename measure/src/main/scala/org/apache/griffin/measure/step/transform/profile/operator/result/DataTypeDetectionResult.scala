package org.apache.griffin.measure.step.transform.profile.operator.result

class DataTypeDetectionResult( var nullCount: Long = 0L,

                               var numericCount: Long = 0L,

                               var dateCount: Long = 0L,

                               var stringCount: Long = 0L) extends Serializable {



}
