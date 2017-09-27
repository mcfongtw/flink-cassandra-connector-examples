/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.streaming.pojo.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.WordCountDataModel;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WordCount;
import org.apache.flink.streaming.connectors.cassandra.example.streaming.CQLPrintSinkFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a streaming windowed version of the "WordCount" program, processing Pojo data type.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class SocketWindowWordCount {
	private static final Logger LOG = LoggerFactory.getLogger(SocketWindowWordCount.class);

	private static final boolean IS_EMBEDDED_CASSANDRA = true;

	public static void main(String[] args) throws Exception {

		// the host and the port to connect to
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
					"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
					"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
					"type the input text into the command line");
			return;
		}

		WordCountDataModel dataModel = new WordCountDataModel();

		dataModel.setUpEmbeddedCassandra();
		dataModel.setUpDataModel();

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordCount> result = text

				.flatMap(new FlatMapFunction<String, WordCount>() {
					@Override
					public void flatMap(String value, Collector<WordCount> out) {
						// normalize and split the line
						String[] words = value.toLowerCase().split("\\s");

						// emit the pairs
						for (String word : words) {
							if (!word.isEmpty()) {
								//Do not accept empty word, since word is defined as primary key in C* table
								out.collect(new WordCount(word, 1L));
							}
						}
					}
				})

				.keyBy("word")
				.timeWindow(Time.seconds(5))

				.reduce(new ReduceFunction<WordCount>() {
					@Override
					public WordCount reduce(WordCount a, WordCount b) {
						return new WordCount(a.getWord(), a.getCount() + b.getCount());
					}
				});

		CassandraSink.addSink(result)
				.setHost("127.0.0.1")
				.build();

		CQLPrintSinkFunction<WordCount, WordCount> func = new CQLPrintSinkFunction();
		func.setDataModel(dataModel, 10);
		result.addSink(func).setParallelism(1);

		env.execute("Socket Window WordCount (POJO) w/ C* Sink");
	}
}
