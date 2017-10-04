/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.factory;

import org.apache.flink.connectors.cassandra.datamodel.AbstractDataModel;
import org.apache.flink.connectors.cassandra.datamodel.WikiEditRecordDataModel;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WikiEditRecord;

/**
 *
 */
public class WikiEditRecordDataModelCreator implements IDataModelCreator<WikiEditRecord> {
    @Override
    public AbstractDataModel<WikiEditRecord> createDataModel() {
        return new WikiEditRecordDataModel();
    }
}
