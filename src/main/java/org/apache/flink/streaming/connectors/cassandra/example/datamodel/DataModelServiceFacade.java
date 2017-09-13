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
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class DataModelServiceFacade {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelServiceFacade.class);

	private EmbeddedCassandraService cassandra = new EmbeddedCassandraService();

	private static final int RECONNECT_DELAY_IN_MS = 100;

	protected Cluster.Builder clientClusterBuilder;

	protected Cluster clientCluster;

	protected Session clientSession;

	protected boolean isEmbeddedCassandra;

	public DataModelServiceFacade() {
	    this(true, "127.0.0.1");
    }

	public DataModelServiceFacade(boolean isEmbedded, String address) {
		clientClusterBuilder = new Cluster.Builder()
                .addContactPoint(address)
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.ONE)
                        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                .withoutJMXReporting()
                .withoutMetrics()
                .withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECT_DELAY_IN_MS));
		isEmbeddedCassandra = isEmbedded;
	}

	public void setUp() throws Exception {
	    if(isEmbeddedCassandra) {
            LOG.info("Bringing up Embedded Cassandra service ... ");
            cassandra.start();
            LOG.info("Bringing up Embedded Cassandra service ... DONE");
        }

		initClientSession();

        addClientShutdownHook();

		initDataModel();
	}

	private void initClientSession() throws Exception {
		Preconditions.checkNotNull(clientClusterBuilder, "Client Cluster Builder");

		long startTimeInMillis = System.currentTimeMillis();
        clientCluster = clientClusterBuilder.build();
        clientSession = clientCluster.connect();

		LOG.info("Client session established after {} ms.", System.currentTimeMillis() - startTimeInMillis);
	}

	private void addClientShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				if (clientSession != null) {
					clientSession.close();
				}

				if (clientCluster != null) {
					clientCluster.close();
				}
			}
		});
	}

	protected abstract void initDataModel();
}



