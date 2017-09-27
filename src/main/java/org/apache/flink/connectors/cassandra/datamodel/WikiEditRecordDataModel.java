/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import org.apache.flink.connectors.cassandra.datamodel.accessor.WikiEditRecordAccessor;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WikiEditRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WikiEditRecordDataModel extends DataModelServiceFacade<WikiEditRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(WikiEditRecordDataModel.class);

    private static final long serialVersionUID = 1L;

    public WikiEditRecordDataModel() {
        this("127.0.0.1");
    }

    public WikiEditRecordDataModel(String address) {
        this(true, address);
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
