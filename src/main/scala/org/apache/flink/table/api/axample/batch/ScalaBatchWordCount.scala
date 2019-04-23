package org.apache.flink.table.api.axample.batch

import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object ScalaBatchWordCount extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = BatchTableEnvironment.create(env)

  val path = this.getClass.getClassLoader.getResource("words.txt").getPath
  tEnv.connect(new FileSystem().path(path))
    .withFormat(new OldCsv().field("line", Types.STRING).lineDelimiter("\n"))
    .withSchema(new Schema().field("line", Types.STRING))
    .registerTableSource("fileSource")

  val resultTable = tEnv.scan("fileSource")
    .groupBy('line)
    .select('line, 'line.count as 'count)

  resultTable.collect().foreach(println)
}

