package org.apache.flink.table.api.example.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class JavaStreamWordCount {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		String path = JavaStreamWordCount.class.getClassLoader().getResource("words.txt").getPath();
		tEnv.connect(new FileSystem().path(path))
			.withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
			.withSchema(new Schema().field("word", Types.STRING))
			.inAppendMode()
			.registerTableSource("fileSource");

		Table result = tEnv.scan("fileSource")
			.groupBy("word")
			.select("word, count(1) as count");

		tEnv.toRetractStream(result, Row.class).print();
		env.execute();
	}
}
