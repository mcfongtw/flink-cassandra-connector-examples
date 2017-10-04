/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.factory;

import org.apache.flink.connectors.cassandra.datamodel.AbstractDataModel;
import org.apache.flink.connectors.cassandra.datamodel.DataEntityType;

/**
 *
 */
public class SimpleDataModelFactory {

    public static AbstractDataModel getDataModel(DataEntityType entityType) {
        if(entityType == DataEntityType.WORD_COUNT ) {
            return new WordCountDataModelCreator().createDataModel();
        } else if(entityType == DataEntityType.WIKI_EDIT_RECORD) {
            return new WikiEditRecordDataModelCreator().createDataModel();
        } else {
            throw new IllegalStateException(entityType.name() + " does not infer to any IDataModelCreator");
        }
    }
}
