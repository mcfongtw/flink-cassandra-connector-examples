/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.streaming.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.DataModelServiceFacade;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WordCount;
import org.apache.flink.util.Collector;

import com.datastax.driver.core.Cluster;
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
 * <li>write flatMap, keyBy and reduce functions,
 * <li>write tuple result back to C* sink
 * </ul>
 */
public class FileWordCount {

	private static final Logger LOG = LoggerFactory.getLogger(FileWordCount.class);

	private static final long serialVersionUID = 1038054554690916991L;

	private static final boolean IS_EMBEDDED_CASSANDRA = true;

	private static class WordCountDataModel extends DataModelServiceFacade {

		public WordCountDataModel() {
			super("127.0.0.1");
		}

		public WordCountDataModel(String address) {
			super(IS_EMBEDDED_CASSANDRA ? "127.0.0.1" : address);
		}

		@Override
		protected void initDataModel() {
			clientSession.execute("CREATE KEYSPACE IF NOT EXISTS " + WordCount.CQL_KEYSPACE_NAME +
					" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" +
					";");
			LOG.info("Keyspace [{}] created", WordCount.CQL_KEYSPACE_NAME);

			clientSession.execute("CREATE TABLE IF NOT EXISTS " + WordCount.CQL_KEYSPACE_NAME + "." + WordCount.CQL_TABLE_NAME +
					"(" +
					"word text, " +
					"count bigint, " +
					"PRIMARY KEY(word)" +
					")" +
					";");

			LOG.info("Table [{}] created", WordCount.CQL_TABLE_NAME);
		}
	}

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

		WordCountDataModel dataModel = new WordCountDataModel();

		if (IS_EMBEDDED_CASSANDRA) {
			dataModel.setUp();
		}

		LOG.info("Example starts!");

		// get input data by reading content from file
		DataStream<String> text = job.readTextFile(inputPath);

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordCount> counts = text
				.flatMap(new FlatMapFunction<String, WordCount>() {
					@Override
					public void flatMap(String value, Collector<WordCount> out) {
						for (String word : value.split("\\s")) {
							out.collect(new WordCount(word, 1L));
						}
					}
				})

				.keyBy("word")

				.reduce(new ReduceFunction<WordCount>() {
					@Override
					public WordCount reduce(WordCount a, WordCount b) {
						return new WordCount(a.getWord(), a.getCount() + b.getCount());
					}
				});

		//Update the results to C* sink
		CassandraSink.addSink(counts)
				.setClusterBuilder(new ClusterBuilder() {
					@Override
					protected Cluster buildCluster(Cluster.Builder builder) {
						return builder.addContactPoint("127.0.0.1").build();
					}
				})
				.build();

		// emit result
		if (outputPath != null) {
			counts.writeAsText(outputPath);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		job.execute("FileWordCount w/ C* Sink and WordCount");

		LOG.info("20 sec sleep ...");
		Thread.sleep(30 * 1000);
		LOG.info("20 sec sleep ... DONE");
	}
}
