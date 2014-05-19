package de.tuberlin.cit.test.queuebehavior.task;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

public class NumberSourceTask extends AbstractGenericInputTask {

	private static final Log LOG = LogFactory.getLog(NumberSourceTask.class);

	private RecordWriter<NumberRecord> out;

	@Override
	public void registerInputOutput() {
		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class);
	}

	@Override
	public void invoke() throws Exception {

		int initalEmitsPerSecond = 10;
		int finalEmitsPerSecond = 100;
		
		int incrementPhaseSteps = 50;
		int incrementPhaseDurationMillis = 200 * 1000;

		// warmup phase
		int emitsPerSecond = initalEmitsPerSecond;
		LOG.info(String.format("Emitting %d recs/sec for %d sec",
				emitsPerSecond, 30));
		doEmits(emitsPerSecond, 30);

		
		// increment phase
		long incrementBeginTime = System.currentTimeMillis();
		
		for (int i = 0; i < incrementPhaseSteps; i++) {
			emitsPerSecond = initalEmitsPerSecond + (int) Math.round((i + 1)
					* (finalEmitsPerSecond - initalEmitsPerSecond)
					/ ((double) incrementPhaseSteps));

			long now = System.currentTimeMillis();
			long nextExpectedNow = Math.round(incrementBeginTime + (i + 1)
					* (incrementPhaseDurationMillis / ((double) incrementPhaseSteps)));

			long durationMillies = nextExpectedNow - now;
			LOG.info(String.format("Emitting %d recs/sec for %.1f sec",
					emitsPerSecond, durationMillies / 1000.0));

			doEmits(emitsPerSecond, durationMillies);
		}

		// final phase
		LOG.info(String.format("Emitting %d recs/sec for %d sec",
				emitsPerSecond, 30));
		doEmits(emitsPerSecond, 30);
	}

	private void doEmits(int emitsPerSecond, long durationMillies)
			throws InterruptedException, IOException {

		long returnTime = System.currentTimeMillis() + durationMillies;
		int counter = 0;

		int avgSleepTime = (int) (1000.0 / emitsPerSecond);
		int recordsEmitted = 0;

		Random rnd = new Random();

		while (true) {
			byte[] nextNumBytes = new byte[32];
			rnd.nextBytes(nextNumBytes);
			BigInteger num = new BigInteger(nextNumBytes).abs();

			NumberRecord toEmit = new NumberRecord();
			toEmit.setNumber(num);

			this.out.emit(toEmit);
			recordsEmitted++;

			if (avgSleepTime > 0) {
				Thread.sleep(avgSleepTime);
			}

			counter++;
			if (counter >= 3) {
				counter = 0;

				long now = System.currentTimeMillis();

				if (now >= returnTime) {
					break;
				}

				long expectedNow = (recordsEmitted - 1) * avgSleepTime;
				if (now < expectedNow) {
					Thread.sleep(expectedNow - now);
					avgSleepTime++;
				} else {
					avgSleepTime = Math.max(avgSleepTime - 1, 0);
				}

			}
		}

	}
}
