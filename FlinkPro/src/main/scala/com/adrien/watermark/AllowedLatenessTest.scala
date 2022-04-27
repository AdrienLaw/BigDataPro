package com.adrien.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowedLatenessTest {
  def main(args: Array[String]): Unit = {
    val enev: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    enev.setParallelism(1)
    val sourceStream: DataStream[String] = enev.socketTextStream("hadoop101", 9909)
    import org.apache.flink.api.scala._
    val mapStream: DataStream[(String, Long)] = sourceStream.map(x => (x.split(",")(0), x.split(",")(1).toLong))

    //定义一个侧输出流
    val outTag = new OutputTag[(String, Long)]("late")
    val resultStream: DataStream[(String, Long)] = mapStream
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      //最大的乱序时间
      val maxOutOfOrderness = 5000L
      //记录最大事件发生时间
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        watermark
      }

      override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
        //获取事件发生时间
        val currentElementEventTime: Long = element._2

        //对比当前事件时间和历史最大事件时间, 将较大值重新赋值给currentMaxTimestamp
        currentMaxTimestamp = Math.max(currentMaxTimestamp, currentElementEventTime)

        println("接受到的事件：" + element + " |事件时间： " + currentElementEventTime)

        currentElementEventTime
      }
    }
    ).keyBy(0)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outTag)
      .process(new ProcessWindowFunction[(String, Long), (String, Long), Tuple, TimeWindow] {
        override def process(key: Tuple, context: Context,
                             elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
          //获取分组的字段
          val value: String = key.getField[String](0)
          //窗口的开始时间
          val startTime: Long = context.window.getStart
          //窗口的结束时间
          val startEnd: Long = context.window.getEnd
          //获取当前的 watermark
          val watermark: Long = context.currentWatermark

          var sum: Long = 0
          val toList: List[(String, Long)] = elements.toList

          for (eachElement <- toList) {
            sum += 1
          }
          println("窗口的数据条数:" + sum +
            " |窗口的第一条数据：" + toList.head +
            " |窗口的最后一条数据：" + toList.last +
            " |窗口的开始时间： " + startTime +
            " |窗口的结束时间： " + startEnd +
            " |当前的watermark:" + watermark)

          out.collect((value, sum))
        }
      })
    resultStream.getSideOutput(outTag).print("outTag")
    resultStream.print("OK")
    enev.execute()
  }
}
