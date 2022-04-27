package com.adrien.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 对socket中有序（按照eventtime时间顺序过来）的数据流，进行每5s处理一次
 */
object OrderedStreamWaterMark {
  def main(args: Array[String]): Unit = {
    val enev:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    enev.setParallelism(1)
    enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceStream: DataStream[String] = enev.socketTextStream("hadoop101", 9909)
    val mapStream : DataStream[(String,Long)] = sourceStream.map(x => (x.split(",")(0) , x.split(",")(1).toLong))
    val watermarkStream: DataStream[(String, Long)] = mapStream.assignAscendingTimestamps(x => x._2)
    watermarkStream.keyBy(0)
        .timeWindow(Time.seconds(5))
        .process(new ProcessWindowFunction[(String,Long),(String,Long),Tuple,TimeWindow] {
            override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
              //todo: 获取分组字段
              val value: String = key.getField[String](0)
              //todo: 窗口的开始时间
              val startTime: Long = context.window.getStart
              //todo: 窗口的结束时间
              val startEnd: Long = context.window.getEnd
              //todo: 获取当前的 watermark
              val watermark: Long = context.currentWatermark
              var sum: Long = 0
              val toList: List[(String, Long)] = elements.toList
              for (eachElement <- toList) {
                sum += 1
              }
              println("窗口的数据条数:" + sum + " |窗口的第一条数据：" + toList.head + " |窗口的最后一条数据：" + toList.last +
                " |窗口的开始时间： " + startTime + " |窗口的结束时间： " + startEnd + " |当前的watermark:" + watermark)

              out.collect((value, sum))
            }
        }).print()
    enev.execute()
  }

}
