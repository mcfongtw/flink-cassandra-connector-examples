/*
 * PUT COPYRIGHT DISCLAIMER / LICENSE HERE
 */

package org.apache.flink.connectors.cassandra.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.connectors.cassandra.datamodel.DataServiceFacade;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.mapping.Result;

import java.io.PrintStream;

/**
 *
 */
public class CQLPrintSinkFunction<IN, M> extends RichSinkFunction<IN>  {

	private static final long serialVersionUID = 1L;

	private static final boolean STD_OUT = false;

	private static final boolean STD_ERR = true;

	private boolean target;

	private transient PrintStream stream;

	private transient String prefix;

	private int selectLimit;

	private DataServiceFacade dataModelService;
	/**
	 * Instantiates a print sink function that prints to standard out.
	 */
	public CQLPrintSinkFunction() {
		this(true);
	}

	/**
	 * Instantiates a print sink function that prints to standard out.
	 *
	 * @param stdErr True, if the format should print to standard error instead of standard out.
	 */
	public CQLPrintSinkFunction(boolean stdErr) {
		target = stdErr;
	}

	public void setDataModel(DataServiceFacade dataService, int limit) {
		dataModelService = dataService;
		selectLimit = limit;
	}

	public void setTargetToStandardOut() {
		target = STD_OUT;
	}

	public void setTargetToStandardErr() {
		target = STD_ERR;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		// get the target stream
		stream = target == STD_OUT ? System.out : System.err;

		// set the prefix if we have a >1 parallelism
		prefix = (context.getNumberOfParallelSubtasks() > 1) ?
				((context.getIndexOfThisSubtask() + 1) + "> ") : null;

		dataModelService.setUpDataModelAccessor();
	}

	@Override
	public void invoke(IN record) {
		if (prefix != null) {
			stream.println(prefix + getAllResultSet());
		} else {
			stream.println(getAllResultSet());
		}
	}

	private String getAllResultSet() {
		Preconditions.checkNotNull(dataModelService, "Data Model Service!");

		Result<M> results = dataModelService.getDataModel().getAccessor().findAll(selectLimit);
		StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------\r\n");
		for (M entity : results) {
			sb.append(entity.toString() + "\r\n");
		}
		sb.append("\r\n");

		return sb.toString();
	}

	@Override
	public void close() {
		if (stream != null) {
			this.stream.close();
		}
		this.prefix = null;
	}

	@Override
	public String toString() {
		return "Print to " + (target == STD_OUT ? "System.out" : "System.err");
	}
}
