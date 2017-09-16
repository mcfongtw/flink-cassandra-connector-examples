/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.streaming.connectors.cassandra.example.datamodel.accessor;

import org.apache.flink.streaming.connectors.cassandra.example.datamodel.DataModelAccessor;
import org.apache.flink.streaming.connectors.cassandra.example.datamodel.pojo.WikiEditRecord;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

/**
 *
 */
@Accessor
public interface WikiEditRecordAccessor extends DataModelAccessor<WikiEditRecord> {

	@Query("SELECT * FROM " + WikiEditRecord.CQL_KEYSPACE_NAME + "." + WikiEditRecord.CQL_TABLE_NAME + " limit :max")
	Result<WikiEditRecord> findAll(@Param("max") int max);
}
