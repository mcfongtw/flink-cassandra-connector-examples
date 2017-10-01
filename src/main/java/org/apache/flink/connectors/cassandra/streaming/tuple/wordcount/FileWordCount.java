/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.streaming.tuple.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connectors.cassandra.datamodel.DataEntityType;
import org.apache.flink.connectors.cassandra.datamodel.DataServiceFacade;
import org.apache.flink.connectors.cassandra.streaming.CQLPrintSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WordCount;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use tuple data types,
 * <li>write flatMap, keyBy and sum functions
 * <li>write tuple result back to C* sink
 * </ul>
 */
public class FileWordCount {
	private static final Logger LOG = LoggerFactory.getLogger(FileWordCount.class);

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment job = StreamExecutionEnvironment.getExecutionEnvironment();
		String inputPath, outputPath = null;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			inputPath = params.get("input");

			if (params.has("output")) {
				outputPath = params.get("output");
			}
			// make parameters available in the web interface
			job.getConfig().setGlobalJobParameters(params);
		} catch (Exception e) {
			System.err.println("No input specified. Please run '" + FileWordCount.class.getSimpleName() +
				"--input <file-path>', where 'input' is the path to a text file");
			return;
		}

		DataServiceFacade dataService = new DataServiceFacade(DataEntityType.WORD_COUNT);

		dataService.setUpEmbeddedCassandra();
		dataService.setUpDataModel();

		LOG.info("Example starts!");

		// get input data by reading content from file
		DataStream<String> text = job.readTextFile(inputPath);

		DataStream<Tuple2<String, Long>> result =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			text.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

				@Override
				public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
					// normalize and split the line
					String[] words = value.toLowerCase().split("\\W+");

					// emit the pairs
					for (String word : words) {
						//Do not accept empty word, since word is defined as primary key in C* table
						if (!word.isEmpty()) {
							out.collect(new Tuple2<String, Long>(word, 1L));
						}
					}
				}
			})
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0)
				.sum(1);

		//Update the results to C* sink
		CassandraSink.addSink(result)
				.setQuery("INSERT INTO " + WordCount.CQL_KEYSPACE_NAME + "." + WordCount.CQL_TABLE_NAME + "(word, count) " +
						"values (?, ?);")
				.setHost("127.0.0.1")
				.build();

		// emit result
		if (outputPath != null) {
			result.writeAsText(outputPath);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");

			CQLPrintSinkFunction<Tuple2<String, Long>, WordCount> func = new CQLPrintSinkFunction();
			func.setDataModel(dataService, 10);
			result.addSink(func).setParallelism(1);
		}

		// execute program
		job.execute("[STREAM] FileWordCount w/ C* Sink");
	}
}
