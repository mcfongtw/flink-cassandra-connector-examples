/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.streaming.pojo.wiki;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connectors.cassandra.datamodel.DataEntityType;
import org.apache.flink.connectors.cassandra.datamodel.DataServiceFacade;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WikiEditRecord;
import org.apache.flink.connectors.cassandra.streaming.CQLPrintSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wikipedia-connector provides means to retrieves all edits to the wiki. In this example, we will stream all edit
 * record to Flink and count the number of bytes that each user edits within a given window of time
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use POJO data types,
 * <li>write timeWindow and fold functions,
 * <li>write POJO result back to C* sink
 * </ul>
 */
public class WikipediaAnalysis {
	private static final Logger LOG = LoggerFactory.getLogger(WikipediaAnalysis.class);

	public static void main(String[] args) throws Exception {

		DataServiceFacade dataService = new DataServiceFacade(DataEntityType.WIKI_EDIT_RECORD);

		dataService.setUpEmbeddedCassandra();
		dataService.setUpDataModel();

		LOG.info("Example starts!");

		StreamExecutionEnvironment job = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<WikipediaEditEvent> edits = job.addSource(new WikipediaEditsSource());

		KeyedStream<WikipediaEditEvent, String> srcStream = edits
			.keyBy(new KeySelector<WikipediaEditEvent, String>() {
				@Override
				public String getKey(WikipediaEditEvent event) {
					return event.getUser();
				}
			});

		DataStream<WikiEditRecord> result = srcStream
			.timeWindow(Time.seconds(5))
			.fold(new WikiEditRecord("DEFAULT", "1970-01-01T00:00:00.000+00:00"), new FoldFunction<WikipediaEditEvent, WikiEditRecord>() {
				@Override
				public WikiEditRecord fold(WikiEditRecord rec, WikipediaEditEvent event) {
					if (!event.getUser().isEmpty()) {
						rec.setUser(event.getUser());
						rec.setDiff(event.getByteDiff());
						rec.setTime(new DateTime(event.getTimestamp()).toString());
						rec.setTitle(event.getTitle());
					}

					return rec;
				}
			});

		CassandraSink.addSink(result)
				.setHost("127.0.0.1")
				.build();

		CQLPrintSinkFunction<WikiEditRecord, WikiEditRecord> func = new CQLPrintSinkFunction();
		func.setDataModel(dataService, 10);
		result.addSink(func).setParallelism(1);

		job.execute("WikiAnalysis w/ C* Sink");
	}
}
