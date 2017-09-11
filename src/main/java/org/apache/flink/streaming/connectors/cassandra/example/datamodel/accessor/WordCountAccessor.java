/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.datamodel.accessor;

import org.apache.flink.streaming.connectors.cassandra.example.datamodel.DataModelAccessor;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WordCount;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Query;

/**
 *
 */
public interface WordCountAccessor extends DataModelAccessor {

	@Query("SELECT * FROM " + WordCount.CQL_KEYSPACE_NAME + "." + WordCount.CQL_TABLE_NAME + " limit 30;")
	Result<WordCount> findAll();
}
