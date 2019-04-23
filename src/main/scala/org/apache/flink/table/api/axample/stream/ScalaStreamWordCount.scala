package org.apache.flink.table.api.axample.stream

import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row

object ScalaStreamWordCount extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = StreamTableEnvironment.create(env)

  val path = this.getClass.getClassLoader.getResource("words.txt").getPath
  tEnv.connect(new FileSystem().path(path))
    .withFormat(new OldCsv().field("line", Types.STRING).lineDelimiter("\n"))
    .withSchema(new Schema().field("line", Types.STRING))
    .inAppendMode()
    .registerTableSource("fileSource")

  val resultTable = tEnv.scan("fileSource")
    .groupBy('line)
    .select('line, 'line.count as 'count)

  implicit val tpe: TypeInformation[Row] = Types.ROW(Types.STRING, Types.LONG) // tpe is automatically
  resultTable.toRetractStream[Row].print()
  env.execute()
}

