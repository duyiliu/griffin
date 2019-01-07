/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.measure.step.transform

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.sink.SinkFactory
import org.apache.griffin.measure.step.DQStep
import org.apache.griffin.measure.step.builder.ConstantColumns
import org.apache.griffin.measure.step.transform.profile.{FieldProfileInfo, InputData, OperatorAccumulator, TableProfileInfo}
import org.apache.griffin.measure.step.transform.profile.operator.{DetailsOperatorParam, Operator, OperatorFactory, OperatorParam}
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.reflect.classTag

/**
  * spark sql transform step
  */
case class CustomOperatorTransformStep(name: String,
                                       rule: String,
                                       details: Map[String, Any],
                                       cache: Boolean = false
                                      ) extends DQStep {


  def execute(context: DQContext): Boolean = {
    val sqlContext = context.sqlContext
    try {

      val source = details.get("datasource").get.toString

      val df = sqlContext.table(source);



      val ruleOperatorParam = JsonUtil.fromJson[DetailsOperatorParam](JsonUtil.toJson(details));



      val structFields = df.schema.fields

      val tableProfileInfo: TableProfileInfo = new TableProfileInfo


      for (i <- 0 until structFields.size) {

        val operatorParams = ruleOperatorParam.fieldOperators.get(structFields(i).name);

        //        structFields(i).name match {
        //          case ConstantColumns.tmst => {
        //            if (operatorParams.isDefined) {
        //              val operators = OperatorFactory.getOperators(operatorParams.get.toIterable);
        //              tableProfileInfo.getFieldProfileInfo(structFields(i).name).get.operators = operators;
        //            } else {
        //              tableProfileInfo.getFieldProfileInfo(structFields(i).name)
        //            }
        //          }
        //
        //
        //        }

        if (!structFields(i).name.equals(ConstantColumns.tmst)) {
          if (operatorParams.isDefined) {
            val operators = OperatorFactory.getOperators(operatorParams.get.toIterable);
            tableProfileInfo.getFieldProfileInfo(structFields(i).name).get.operators = operators;
          } else {
            tableProfileInfo.getFieldProfileInfo(structFields(i).name)
          }
        }

      }

      var operatorAccumulator = new OperatorAccumulator(tableProfileInfo);

      // val structFields = df.schema.fields

      sqlContext.sparkContext.register(operatorAccumulator)

      var count = df.rdd.map(f => {


        for (i <- 0 until structFields.size) {


          val inputData = new InputData(structFields(i).name, f.get(i), tableProfileInfo.getFieldProfileInfo(structFields(i).name).get.operators);


          operatorAccumulator.add(inputData)


        }


      }).count()

      var countApproxDistinct = df.rdd.countApproxDistinct();

      operatorAccumulator.value.rowCount = count;
      operatorAccumulator.value.repeatCount = if (count - countApproxDistinct > 0) count - countApproxDistinct else 0;


      println("operatorAccumulator:" + JsonUtil.toJson(operatorAccumulator.value))

      println("count:" + count)

      val schema = StructType(List(
        StructField("tableProfileInfo", StringType, nullable = true)
      ))
      //
      val rdd = sqlContext.sparkContext.parallelize(Seq(
        Row(JsonUtil.toJson(operatorAccumulator.value))
      ))
      val df2 = sqlContext.createDataFrame(rdd, schema)
      //
      //      //   val df = sqlContext.sql(rule)
      //      if (cache) context.dataFrameCache.cacheDataFrame(name, df2)
      context.runTimeTableRegister.registerTable(name, df2)
      true
    } catch {
      case e: Throwable =>
        error(s"run spark sql [ ${rule} ] error: ${e.getMessage}", e)
        false
    }
  }

}
