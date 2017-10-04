/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.factory;

import org.apache.flink.connectors.cassandra.datamodel.AbstractDataModel;

/**
 *
 */
public interface IDataModelCreator<M> {

    AbstractDataModel<M> createDataModel();
}
