package com.adrien.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object FlinkTableDemo {
  case class User(id:Int,name:String,age:Int)

  def main(args: Array[String]): Unit = {
    val enev: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val enevTab: StreamTableEnvironment = StreamTableEnvironment.create(enev)

    val socketStream: DataStream[String] = enev.socketTextStream("node01",9999)
    import org.apache.flink.api.scala._
    val userStream: DataStream[User] = socketStream.map(x => x.split(","))
      .map(x => User(x(0).toInt, x(1), x(2).toInt))

    enevTab.registerDataStream("userTable",userStream)
    val result: Table = enevTab.scan("userTable").filter("age >20")
    enevTab.toAppendStream[Row](result).print()
    enevTab.execute("ad")
  }

}
