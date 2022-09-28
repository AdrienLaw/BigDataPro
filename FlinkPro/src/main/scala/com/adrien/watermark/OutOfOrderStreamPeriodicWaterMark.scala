package com.adrien.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 对无序的数据流周期性的添加水印
 * todo: 对无序或者是延迟的数据来实现watermark+ eventtime进行正确的处理
 */
object OutOfOrderStreamPeriodicWaterMark {
  def main(args: Array[String]): Unit = {
    //1. 执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    environment.setParallelism(1)
    //输入流
    val sourceStream: DataStream[String] = environment.socketTextStream("hadoop101", 9909)
    //Mao
    val mapStream: DataStream[(String, Long)] = sourceStream.map(x => (x.split(",")(0), x.split(",")(1).toLong))
    /**
     * 水位线 = 时间最大发生时间 - 最大允许迟到时间
     */
    mapStream.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[(String,Long)] {
        //最大的乱序时间
        val delayTime:Long=5000L
        //最大的事件发生时间
        var maxEventTime:Long=_

        //周期性的生成水位线watermark
        override def getCurrentWatermark: Watermark = {
          val watermark = new Watermark(maxEventTime - delayTime)
          watermark
        }

        //抽取事件发生时间
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
          //获取事件发生时间
          val eventTime: Long = element._2
          //对比当前事件时间和最大的事件发生时间, 将较大值重新赋值给maxEventTime
          maxEventTime=  Math.max(maxEventTime,eventTime)
          // 返回事件发生时间
          eventTime
        }
      }
    ).keyBy(0)
      .timeWindow(Time.seconds(5))
      .process(new ProcessWindowFunction[(String, Long),(String,Long),Tuple,TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
          //获取分组字段
          val value: String = key.getField[String](0)
          //窗口的开始时间
          val startTime: Long = context.window.getStart
          //窗口的结束时间
          val startEnd: Long = context.window.getEnd
          //获取当前的 watermark
          val watermark: Long = context.currentWatermark
          var sum:Long = 0
          val toList: List[(String, Long)] = elements.toList
          for(eachElement <-  toList){
            sum +=1
          }

          println("窗口的数据条数:"+sum+
            " |窗口的第一条数据："+toList.head+
            " |窗口的最后一条数据："+toList.last+
            " |窗口的开始时间： "+  startTime +
            " |窗口的结束时间： "+ startEnd+
            " |当前的watermark:"+watermark)

          out.collect((value,sum))
        }
      }).print()
    environment.execute()

  }

}
