/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.factory;

import org.apache.flink.connectors.cassandra.datamodel.AbstractDataModel;
import org.apache.flink.connectors.cassandra.datamodel.WordCountDataModel;
import org.apache.flink.connectors.cassandra.datamodel.pojo.WordCount;

/**
 *
 */
public class WordCountDataModelCreator implements IDataModelCreator<WordCount> {
    @Override
    public AbstractDataModel<WordCount> createDataModel() {
        return new WordCountDataModel();
    }
}
