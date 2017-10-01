/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.batch.tuple.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.connectors.cassandra.datamodel.DataEntityType;
import org.apache.flink.connectors.cassandra.datamodel.DataServiceFacade;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WordCount;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;

import com.datastax.driver.core.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FileWordCount {
	private static final Logger LOG = LoggerFactory.getLogger(FileWordCount.class);

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final ExecutionEnvironment job = ExecutionEnvironment.getExecutionEnvironment();
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
			System.err.println("No input specified. Please run '" + org.apache.flink.connectors.cassandra.streaming.tuple.wordcount.FileWordCount.class.getSimpleName() +
					"--input <file-path>', where 'input' is the path to a text file");
			return;
		}

		DataServiceFacade dataService = new DataServiceFacade(DataEntityType.WORD_COUNT);

		dataService.setUpEmbeddedCassandra();
		dataService.setUpDataModel();

		LOG.info("Example starts!");

		// get input data by reading content from file
		DataSet<String> text = job.readTextFile(inputPath);

		DataSet<Tuple2<String, Long>> result =
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
                .groupBy(0)
                .sum(1);

		//Update the results to C* sink
		CassandraOutputFormat sink = new CassandraOutputFormat("INSERT INTO " + WordCount.CQL_KEYSPACE_NAME + "." + WordCount.CQL_TABLE_NAME + "(word, count) " +
				"values (?, ?);", new ClusterBuilder() {
			@Override
			protected Cluster buildCluster(Cluster.Builder builder) {
				builder.addContactPoint("127.0.0.1");
				return builder.build();
			}
		});

		result.output(sink);

		// emit result
		if (outputPath != null) {
			result.writeAsText(outputPath);
		}

		// execute program
		job.execute("[BATCH] FileWordCount w/ C* Sink");

		LOG.info("20 sec sleep ...");
		Thread.sleep(20 * 1000);
		LOG.info("20 sec sleep ... DONE");
	}
}
