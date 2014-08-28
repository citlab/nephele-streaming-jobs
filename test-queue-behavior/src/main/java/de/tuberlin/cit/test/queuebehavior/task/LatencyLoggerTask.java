package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.util.LatencyLog;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

public class LatencyLoggerTask extends AbstractOutputTask {

	public final static String LATENCY_LOG_KEY = "receivertask.latency.log";

	public final static String LATENCY_LOG_DEFAULT = "/tmp/qos_statistics_receiver";

	private RecordReader<NumberRecord> in;

	@Override
	public void registerInputOutput() {
		this.in = new RecordReader<NumberRecord>(this, NumberRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		LatencyLog latLogger = null;
		try {
			latLogger = new LatencyLog(getTaskConfiguration().getString(
					LATENCY_LOG_KEY, LATENCY_LOG_DEFAULT));
			while (this.in.hasNext()) {
				NumberRecord record = this.in.next();
				latLogger.log(record.getTimestamp());
			}
		} finally {
			if (latLogger != null) {
				latLogger.close();
			}
		}
	}

}
