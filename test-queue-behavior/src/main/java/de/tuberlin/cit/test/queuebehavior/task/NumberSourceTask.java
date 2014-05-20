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
		int warmupPhaseDurationMillis = 30 * 1000;
		int finalPhaseDurationMillis = 30 * 1000;

		int initalEmitsPerSecond = 500;
		int finalEmitsPerSecond = 5000;

		int incrementPhaseSteps = 20;
		int incrementPhaseDurationMillis = 120 * 1000;

		// warmup phase
		int emitsPerSecond = initalEmitsPerSecond;
		LOG.info(String.format("Emitting %d recs/sec for %.1f sec",
				emitsPerSecond, warmupPhaseDurationMillis / 1000.0));
		doEmits(emitsPerSecond, warmupPhaseDurationMillis);

		// increment phase
		long incrementBeginTime = System.currentTimeMillis();

		for (int i = 0; i < incrementPhaseSteps; i++) {
			emitsPerSecond = initalEmitsPerSecond
					+ (int) Math.round((i + 1)
							* (finalEmitsPerSecond - initalEmitsPerSecond)
							/ ((double) incrementPhaseSteps));

			long now = System.currentTimeMillis();
			long nextExpectedNow = Math
					.round(incrementBeginTime
							+ (i + 1)
							* (incrementPhaseDurationMillis / ((double) incrementPhaseSteps)));

			long durationMillies = nextExpectedNow - now;
			LOG.info(String.format("Emitting %d recs/sec for %.1f sec",
					emitsPerSecond, durationMillies / 1000.0));

			doEmits(emitsPerSecond, durationMillies);
		}

		// final phase
		LOG.info(String.format("Emitting %d recs/sec for %.1f sec",
				emitsPerSecond, finalPhaseDurationMillis / 1000.0));
		doEmits(emitsPerSecond, finalPhaseDurationMillis);
	}

	private void doEmits(int emitsPerSecond, long durationMillies)
			throws InterruptedException, IOException {

		long beginTime = System.currentTimeMillis();
		long returnTime = beginTime + durationMillies;
		int counter = 0;

		int sleepTime = (int) (1000.0 / emitsPerSecond);

		int totalEmits = (int) Math.round(emitsPerSecond
				* (durationMillies / 1000.0));
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

			if (sleepTime > 0) {
				Thread.sleep(sleepTime);
			}

			counter++;
			if (counter >= 5) {
				counter = 0;

				long now = System.currentTimeMillis();

				if (now >= returnTime) {
					double secsPassed = (now - beginTime) / 1000.0;
					LOG.info(String
							.format("Emitted %.1f recs/sec for records for %.1f sec (%d records total)",
									recordsEmitted / secsPassed, secsPassed,
									recordsEmitted));
					break;
				}

				int expectedEmitted = (int) (totalEmits * ((now - beginTime) / ((double) durationMillies)));
				if (recordsEmitted > expectedEmitted) {
					sleepTime++;
				} else if (recordsEmitted < expectedEmitted && sleepTime > 0) {
					sleepTime--;
				}

			}
		}

	}
}
