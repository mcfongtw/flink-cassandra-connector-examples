/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.streaming.pojo.wiki;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.example.streaming.CQLPrintSinkFunction;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.DataModelServiceFacade;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.accessor.WikiEditRecordAccessor;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WikiEditRecord;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import com.datastax.driver.core.Cluster;
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

	private static final boolean IS_EMBEDDED_CASSANDRA = true;

	private static class WikiEditRecordDataModel extends DataModelServiceFacade<WikiEditRecord> {

		private static final long serialVersionUID = 1L;

		public WikiEditRecordDataModel() {
			this("127.0.0.1");
		}

		public WikiEditRecordDataModel(String address) {
			this(IS_EMBEDDED_CASSANDRA, address);
		}

		public WikiEditRecordDataModel(boolean isEmbedded, String address) {
			super(isEmbedded, address, WikiEditRecordAccessor.class);
		}

		@Override
		protected void initDataModel() {
			clientSession.execute("CREATE KEYSPACE IF NOT EXISTS " + WikiEditRecord.CQL_KEYSPACE_NAME + " " +
				"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" +
				";");
			LOG.info("Keyspace [{}] created", WikiEditRecord.CQL_KEYSPACE_NAME);

			clientSession.execute("CREATE TABLE IF NOT EXISTS " + WikiEditRecord.CQL_KEYSPACE_NAME + "." + WikiEditRecord.CQL_TABLE_NAME +
				"(" +
				"user text, " +
				"time text, " +
				"diff bigint," +
				"title text," +
				"PRIMARY KEY(user, time)" +
				") WITH CLUSTERING ORDER BY (time DESC)" +
				";");
			LOG.info("Table [{}] created", WikiEditRecord.CQL_TABLE_NAME);
		}
	}

	public static void main(String[] args) throws Exception {

		WikiEditRecordDataModel dataModel = new WikiEditRecordDataModel();

		dataModel.setUpEmbeddedCassandra();
		dataModel.setUpDataModel();

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
			.setClusterBuilder(new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Cluster.Builder builder) {
					return builder.addContactPoint("127.0.0.1").build();
				}
			})
			.build();

		CQLPrintSinkFunction<WikiEditRecord, WikiEditRecord> func = new CQLPrintSinkFunction();
		func.setDataModel(dataModel, 10);
		result.addSink(func).setParallelism(1);

		job.execute("WikiAnalysis w/ C* Sink");
	}
}
