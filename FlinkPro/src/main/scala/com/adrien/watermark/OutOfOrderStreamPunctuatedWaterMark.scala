package com.adrien.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object OutOfOrderStreamPunctuatedWaterMark {
  def main(args: Array[String]): Unit = {
    val enev: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    enev.setParallelism(1)
    enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = enev.socketTextStream("hadoop101", 9909)

    val mapStream: DataStream[(String, Long)] = sourceStream.map(x=>(x.split(",")(0),x.split(",")(1).toLong))
    mapStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {
      //定义数据乱序的最大时间
      val maxOutOfOrderness=5000L

      //最大事件发生时间
      var currentMaxTimestamp:Long=_
      override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
        //当用户id为000001生成watermark
        if (lastElement._1.equals("000001")) {
          val watermark=  new Watermark(currentMaxTimestamp - maxOutOfOrderness)
          watermark
        } else {
          //其他情况下不返回水位线
          null
        }
      }

      override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
        //获取事件发生时间
        val currentElementEventTime: Long = element._2
        //对比当前事件时间和历史最大事件时间, 将较大值重新赋值给currentMaxTimestamp
        currentMaxTimestamp=Math.max(currentMaxTimestamp,currentElementEventTime)

        println("接受到的事件："+element+" |事件时间： "+currentElementEventTime )
        //返回事件发生时间
        currentElementEventTime
      }
    })
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .process(new ProcessWindowFunction[(String, Long),(String,Long),Tuple,TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
          //获取分组的字段
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
            " |窗口的开始时间： "+startTime +
            " |窗口的结束时间： "+startEnd+
            " |当前的watermark:"+watermark)

          out.collect((value,sum))
        }
      }).print()
    enev.execute()
  }

}
