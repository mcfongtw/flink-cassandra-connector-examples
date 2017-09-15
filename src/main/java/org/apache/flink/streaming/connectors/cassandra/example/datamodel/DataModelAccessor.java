/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.datamodel;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;

/**
 *
 */
@Accessor
public interface DataModelAccessor<M> {

	Result<M> findAll(int max);
}
