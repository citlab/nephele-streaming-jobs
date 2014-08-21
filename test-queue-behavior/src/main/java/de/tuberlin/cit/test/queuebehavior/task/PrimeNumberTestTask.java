package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.record.NumberRecord.Primeness;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;

public class PrimeNumberTestTask extends AbstractTask {

	private RecordReader<NumberRecord> in;

	private RecordWriter<NumberRecord> out;

	@Override
	public void registerInputOutput() {
		this.in = new RecordReader<NumberRecord>(this, NumberRecord.class);
		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		while (in.hasNext()) {
			NumberRecord num = this.in.next();
			if (num.getNumber().isProbablePrime(100)) {
				num.setPrimeness(Primeness.PRIME);
			} else {
				num.setPrimeness(Primeness.NOT_PRIME);
			}
			this.out.emit(num);
		}
	}
}
