/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import org.apache.flink.connectors.cassandra.EmbeddedCassandraService;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.mapping.MappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 *
 */
public abstract class DataModelServiceFacade<M> implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DataModelServiceFacade.class);

	protected boolean isEmbeddedCassandra;

	protected String hostAddr;

	private static final int RECONNECT_DELAY_IN_MS = 1000 * 10;

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	private transient EmbeddedCassandraService cassandra = new EmbeddedCassandraService();

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	protected transient Cluster clientCluster;

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	protected transient Session clientSession;

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	protected transient MappingManager mappingMgr;

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	protected transient DataModelAccessor dataModelAccessor;

	protected Class<? extends DataModelAccessor> accessorClass;

	public DataModelServiceFacade(boolean isEmbedded, String address, Class<? extends DataModelAccessor> clazz) {
		isEmbeddedCassandra = isEmbedded;
		hostAddr = address;
		accessorClass = clazz;
	}

	public void setUpEmbeddedCassandra() throws Exception {
		if (isEmbeddedCassandra) {
			LOG.info("Bringing up Embedded Cassandra service ... ");
			cassandra.start();
			LOG.info("Bringing up Embedded Cassandra service ... DONE");
		}
	}

	public void setUpDataModel() throws Exception {
		initClientSession();

		initDataModel();
	}

	public void setUpDataModelAccessor() throws Exception {
		initClientSession();

		initDataModelAccessor();
	}

	private void initClientSession() throws Exception {
        Cluster.Builder clientClusterBuilder = new Cluster.Builder()
				.addContactPoint(hostAddr)
				.withQueryOptions(new QueryOptions()
						.setConsistencyLevel(ConsistencyLevel.ONE)
						.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
				.withoutJMXReporting()
				.withoutMetrics()
				.withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECT_DELAY_IN_MS));
		long startTimeInMillis = System.currentTimeMillis();
		clientCluster = clientClusterBuilder.build();
		clientSession = clientCluster.connect();
		mappingMgr = new MappingManager(clientSession);

		LOG.info("Client session established after {} ms.", System.currentTimeMillis() - startTimeInMillis);

		addClientShutdownHook();
	}

	private void initDataModelAccessor() {
		Preconditions.checkNotNull(clientSession, "Session cannot be null!");
		Preconditions.checkNotNull(mappingMgr, "Object mapping manager cannot be null!");
		Preconditions.checkNotNull(accessorClass, "accessorClass cannot be null!");

		dataModelAccessor = mappingMgr.createAccessor(accessorClass);
	}

	public DataModelAccessor getDataModelAccessor() {
		return dataModelAccessor;
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



