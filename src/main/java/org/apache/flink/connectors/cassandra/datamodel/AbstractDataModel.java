/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import org.apache.flink.connectors.cassandra.datamodel.accessor.DataModelAccessor;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 *
 */
public abstract class AbstractDataModel<M> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DataServiceFacade.class);

    private static final long serialVersionUID = 1L;

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

    public AbstractDataModel(Class<? extends DataModelAccessor> clazz) {
        accessorClass = clazz;
    }

    public void initClientSession(String dbAddr) throws Exception {
        clientSession = ClientSessionProvider.getClientSession(dbAddr);
        mappingMgr = new MappingManager(clientSession);

        addClientShutdownHook();
    }

    public void initDataModelAccessor() {
        Preconditions.checkNotNull(clientSession, "Session cannot be null!");
        Preconditions.checkNotNull(mappingMgr, "Object mapping manager cannot be null!");
        Preconditions.checkNotNull(accessorClass, "accessorClass cannot be null!");

        dataModelAccessor = mappingMgr.createAccessor(accessorClass);
    }

    public DataModelAccessor getAccessor() {
        return dataModelAccessor;
    }

    private void addClientShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                if (clientSession != null) {
                    clientSession.close();
                }

                if (clientSession.getCluster() != null) {
                    clientSession.getCluster().close();
                }

            }
        });
    }

    //TODO: https://stackoverflow.com/questions/44950245/generate-a-script-to-create-a-table-from-the-entity-definition/45073242#45073242
    //Java-Driver > 3.x to use MappedProperty
    protected abstract void initDataModel();
}
