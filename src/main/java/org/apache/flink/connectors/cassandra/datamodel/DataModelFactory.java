/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

/**
 *
 */
public class DataModelFactory {

    public static AbstractDataModel getDataModel(String dbAddress, DataEntityType entityType) {
        if(entityType == DataEntityType.WORD_COUNT ) {
            return new WordCountDataModel();
        } else if(entityType == DataEntityType.WIKI_EDIT_RECORD) {
            return new WikiEditRecordDataModel();
        } else {
            throw new IllegalStateException(entityType.name() + " does not exist");
        }
    }
}
