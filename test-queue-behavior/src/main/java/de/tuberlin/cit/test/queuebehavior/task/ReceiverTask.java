package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

public class ReceiverTask extends AbstractOutputTask {

	private RecordReader<NumberRecord> in;

	@Override
	public void registerInputOutput() {
		this.in = new RecordReader<NumberRecord>(this, NumberRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		while(this.in.hasNext()) {
			this.in.next();
		}
	}
}
