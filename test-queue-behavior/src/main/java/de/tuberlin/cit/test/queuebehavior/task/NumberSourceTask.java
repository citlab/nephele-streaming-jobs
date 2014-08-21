package de.tuberlin.cit.test.queuebehavior.task;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile;
import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile.LoadGenerationProfile;
import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import eu.stratosphere.nephele.io.OpportunisticRoundRobinChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

public class NumberSourceTask extends AbstractGenericInputTask {

	private static final Log LOG = LogFactory.getLog(NumberSourceTask.class);

	public static final String PROFILE_PROPERTY_KEY = "TEST_QUEUE_JOB_PROFILE";
	
	public static final String PROFILE_PROPERTY_DEFAULT = TestQueueBehaviorJobProfile.LOCAL_DUALCORE.name;

	private RecordWriter<NumberRecord> out;

	@Override
	public void registerInputOutput() {
		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class,
				new OpportunisticRoundRobinChannelSelector<NumberRecord>(
						getCurrentNumberOfSubtasks(), getIndexInSubtaskGroup()));
	}

	@Override
	public void invoke() throws Exception {
		Thread.sleep(5000);
		
		LoadGenerationProfile profile = TestQueueBehaviorJobProfile.PROFILES
				.get(getTaskConfiguration().getString(PROFILE_PROPERTY_KEY,
						PROFILE_PROPERTY_DEFAULT)).loadGenProfile;

		// warmup phase

		doEmits(profile.minEmitsPerSecond, profile.warmupPhaseDurationMillis);

		// increment phase
		doIncrementPhase(profile.minEmitsPerSecond, profile.maxEmitsPerSecond,
				profile.incrementPhaseSteps, profile.incrementPhaseDurationMillis);
		
		// plateau phase
		doEmits(profile.maxEmitsPerSecond, profile.plateauPhaseDurationMillis);
		
		// decrement phase
		doDecrementPhase(profile.minEmitsPerSecond, profile.maxEmitsPerSecond,
				profile.decrementPhaseSteps, profile.decrementPhaseDurationMillis);

		// final phase
		doEmits(profile.minEmitsPerSecond, profile.finalPhaseDurationMillis);
	}

	private void doDecrementPhase(int minEmitsPerSecond, int maxEmitsPerSecond,
			int decrementPhaseSteps, int decrementPhaseDurationMillis)
			throws InterruptedException, IOException {

		long decrementBeginTime = System.currentTimeMillis();
		for (int i = 0; i < decrementPhaseSteps; i++) {
			int emitsPerSecond = maxEmitsPerSecond
					- (int) Math.round(i
							* (maxEmitsPerSecond - minEmitsPerSecond)
							/ ((double) decrementPhaseSteps));

			long now = System.currentTimeMillis();
			long nextExpectedNow = Math
					.round(decrementBeginTime
							+ (i + 1)
							* (decrementPhaseDurationMillis / ((double) decrementPhaseSteps)));

			long durationMillies = nextExpectedNow - now;

			doEmits(emitsPerSecond, durationMillies);
		}
	}

	private void doIncrementPhase(int minEmitsPerSecond, int maxEmitsPerSecond,
			int incrementPhaseSteps, int incrementPhaseDurationMillis)
			throws InterruptedException, IOException {
		
		long incrementBeginTime = System.currentTimeMillis();
		for (int i = 0; i < incrementPhaseSteps; i++) {
			int emitsPerSecond = minEmitsPerSecond
					+ (int) Math.round((i + 1)
							* (maxEmitsPerSecond - minEmitsPerSecond)
							/ ((double) incrementPhaseSteps));

			long now = System.currentTimeMillis();
			long nextExpectedNow = Math
					.round(incrementBeginTime
							+ (i + 1)
							* (incrementPhaseDurationMillis / ((double) incrementPhaseSteps)));

			long durationMillies = nextExpectedNow - now;

			doEmits(emitsPerSecond, durationMillies);
		}
	}

	private void doEmits(int emitsPerSecond, long durationMillies)
			throws InterruptedException, IOException {

		LOG.info(String.format("Emitting %d recs/sec for %.1f sec",
				emitsPerSecond, durationMillies / 1000.0));

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
					double actualEmitsPerSecond = recordsEmitted / secsPassed;

					LOG.info(String
							.format("Emitted %.1f recs/sec for records for %.1f sec (%d records total)",
									actualEmitsPerSecond, secsPassed,
									recordsEmitted));

					LOG.info(String.format("qb: %d;%d;%d", beginTime / 1000,
							emitsPerSecond, (int) actualEmitsPerSecond));

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
