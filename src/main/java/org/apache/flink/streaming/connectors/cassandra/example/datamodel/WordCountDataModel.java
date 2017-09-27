/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.datamodel;

import org.apache.flink.streaming.connectors.cassandra.example.datamodel.accessor.WordCountAccessor;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WordCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class WordCountDataModel extends DataModelServiceFacade<WordCount> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountDataModel.class);

    private static final long serialVersionUID = 1L;

    public WordCountDataModel(String address) {
        this(true, address);
    }

    public WordCountDataModel() {
        this("127.0.0.1");
    }

    public WordCountDataModel(boolean isEmbedded, String address) {
        super(isEmbedded, address, WordCountAccessor.class);
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
