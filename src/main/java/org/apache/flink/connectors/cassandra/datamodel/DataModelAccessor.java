/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;

/**
 *
 */
@Accessor
public interface DataModelAccessor<M> {

	Result<M> findAll(int max);
}
