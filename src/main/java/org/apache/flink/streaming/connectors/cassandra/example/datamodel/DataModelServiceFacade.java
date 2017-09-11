/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.datamodel;

import org.apache.flink.streaming.connectors.cassandra.example.EmbeddedCassandraService;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class DataModelServiceFacade {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelServiceFacade.class);

	private EmbeddedCassandraService cassandra = new EmbeddedCassandraService();

	protected Cluster.Builder clientClusterBuilder;

	protected Cluster clientCluster;

	protected Session clientSession;

	public DataModelServiceFacade(String address) {
		clientClusterBuilder = new Cluster.Builder().addContactPoint(address).withQueryOptions(new QueryOptions()
			.setConsistencyLevel(ConsistencyLevel.ONE).setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
			.withoutJMXReporting()
			.withoutMetrics();
	}

	public void setUp() throws Exception {
		LOG.info("Bringing up Embedded Cassandra service");

		cassandra.start();

		initClientSession();

		addShutdownHook();

		initDataModel();
	}

	private void initClientSession() throws Exception {
		Preconditions.checkNotNull(clientClusterBuilder, "Client Cluster Builder");

		// start establishing a connection within 30 seconds
		long startTimeInMillis = System.currentTimeMillis();
		long maxWaitInMillis = 30 * 1000;
		do {
			try {
				clientCluster = clientClusterBuilder.build();
				clientSession = clientCluster.connect();
				break;
			} catch (Exception e) {
				long elapsedTimeInMillis = System.currentTimeMillis() - startTimeInMillis;
				if (elapsedTimeInMillis > maxWaitInMillis) {
					throw e;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ignored) {
				}
			}
		} while(true);

		LOG.info("Client session established after {}ms.", System.currentTimeMillis() - startTimeInMillis);
	}

	public void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				if (clientSession != null) {
					clientSession.close();
				}

				if (clientCluster != null) {
					clientCluster.close();
				}

				if (cassandra != null) {
					cassandra.stop();
				}
			}
		});
	}

	protected abstract void initDataModel();
}



