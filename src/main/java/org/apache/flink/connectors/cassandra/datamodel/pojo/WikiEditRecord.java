/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.datamodel.pojo;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 *
 *
 */
@Table(keyspace = WikiEditRecord.CQL_KEYSPACE_NAME, name = WikiEditRecord.CQL_TABLE_NAME)
public class WikiEditRecord {
	public static final String CQL_KEYSPACE_NAME = "flink";

	public static final String CQL_TABLE_NAME = "wikiedit";

	@Column(name = "user")
	private String user = "";

	@Column(name = "diff")
	private long diff = 0;

	@Column(name = "title")
	private String title = "";

	@Column(name = "time")
	private String time = "";

	public WikiEditRecord() {}

	public WikiEditRecord(String user, String time) {
		this.setUser(user);
		this.setTime(time);
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getDiff() {
		return diff;
	}

	public void setDiff(long diff) {
		this.diff = diff;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("User: " + getUser() + "\t | ");
		builder.append("Time: " + getTime() + "\t | ");
		builder.append("Title: " + getTitle() + "\t | ");
		builder.append("Diff: " + getDiff() + " bytes|");
		return builder.toString();
	}
}
