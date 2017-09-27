/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import org.apache.flink.connectors.cassandra.EmbeddedCassandraService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 *
 */
public class DataServiceFacade implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DataServiceFacade.class);

    private static final long serialVersionUID = 1L;

	private static final String LOCALHOST_ADDR = "127.0.0.1";

	protected boolean isEmbeddedCassandra;

	protected AbstractDataModel dataModel;

	/**
	 * prevent from being serialized as part of SinkFunction.
	 */
	private transient EmbeddedCassandraService cassandra = new EmbeddedCassandraService();


	protected Class<? extends DataModelAccessor> accessorClass;

    public DataServiceFacade(DataEntityType entityType) {
        this(true, LOCALHOST_ADDR, entityType);
    }

	public DataServiceFacade(boolean isEmbedded, String dbAddr, DataEntityType entityType) {
		isEmbeddedCassandra = isEmbedded;
		dataModel = DataModelFactory.getDataModel(dbAddr, entityType);
	}

	public void setUpEmbeddedCassandra() throws Exception {
		if (isEmbeddedCassandra) {
			LOG.info("Bringing up Embedded Cassandra service ... ");
			cassandra.start();
			LOG.info("Bringing up Embedded Cassandra service ... DONE");
		}
	}

	public void setUpDataModel() throws Exception {
        this.setUpDataModel(LOCALHOST_ADDR);
    }

	public void setUpDataModel(String dbAddr) throws Exception {
        dataModel.initClientSession(dbAddr);

        dataModel.initDataModel();
	}

	public void setUpDataModelAccessor() throws Exception {
        this.setUpDataModelAccessor(LOCALHOST_ADDR);
    }

	public void setUpDataModelAccessor(String dbAddr) throws Exception {
        dataModel.initClientSession(dbAddr);

        dataModel.initDataModelAccessor();
	}

	public AbstractDataModel getDataModel() {
        return dataModel;
    }

}



