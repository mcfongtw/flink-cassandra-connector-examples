/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.pojo;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * WordCount Pojo with DataStax annotations used.
 */
@Table(keyspace = WordCount.CQL_KEYSPACE_NAME, name = WordCount.CQL_TABLE_NAME)
public class WordCount {

	public static final String CQL_KEYSPACE_NAME = "flink";

	public static final String CQL_TABLE_NAME = "wordcount";

	@Column(name = "word")
	private String word = "";

	@Column(name = "count")
	private long count = 0;

	public WordCount() {}

	public WordCount(String word, long count) {
		this.setWord(word);
		this.setCount(count);
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return getWord() + " : " + getCount();
	}
}
