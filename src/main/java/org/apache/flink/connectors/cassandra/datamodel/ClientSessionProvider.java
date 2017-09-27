/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ClientSessionProvider {
    private static Logger LOG = LoggerFactory.getLogger(ClientSessionProvider.class);

    private static final int RECONNECT_DELAY_IN_MS = 1000 * 10;

    private static Map<String, Session> REGISTRY = new ConcurrentHashMap<>();

    public static Session getClientSession(String hostAddr) {
        if(REGISTRY.containsKey(hostAddr)) {
            return REGISTRY.get(hostAddr);
        } else {
            Cluster.Builder clientClusterBuilder = new Cluster.Builder()
                    .addContactPoint(hostAddr)
                    .withQueryOptions(new QueryOptions()
                            .setConsistencyLevel(ConsistencyLevel.ONE)
                            .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL))
                    .withoutJMXReporting()
                    .withoutMetrics()
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(RECONNECT_DELAY_IN_MS));
            long startTimeInMillis = System.currentTimeMillis();
            Cluster clientCluster = clientClusterBuilder.build();
            Session clientSession = clientCluster.connect();

            LOG.info("Client session established after {} ms.", System.currentTimeMillis() - startTimeInMillis);
            REGISTRY.putIfAbsent(hostAddr, clientSession);
            return clientSession;
        }
    }
}
