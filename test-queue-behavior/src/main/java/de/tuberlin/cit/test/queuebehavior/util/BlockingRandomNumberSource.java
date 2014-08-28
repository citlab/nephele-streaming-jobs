package de.tuberlin.cit.test.queuebehavior.util;

import java.math.BigInteger;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile.LoadGenerationProfile;

public class BlockingRandomNumberSource {
	
	private static final Logger LOG = LoggerFactory.getLogger(BlockingRandomNumberSource.class);

	public static class TimestampedNumber {
		public long timestamp;
		public BigInteger number;
	}

	private LoadGenerationProfile profile;

	private enum LoadGenPhase {
		INITIAL_SLEEP, WARMUP, INCREMENT, PLATEAU, DECREMENT, COOLDOWN, DONE
	}

	private LoadGenPhase currPhase;

	private int currPhaseTotalSteps;

	private int currPhaseStep;

	private long currPhaseStepBeginTime;

	private long currPhaseStepEndTime;

	private long currPhaseStepDuration;

	private int currPhaseStepTotalEmits;

	private int currPhaseStepEmits;

	private int sleepTime = 0;

	private final Random rnd = new Random();

	public BlockingRandomNumberSource(LoadGenerationProfile profile) {
		this.profile = profile;
		currPhase = LoadGenPhase.INITIAL_SLEEP;
		currPhaseStep = 0;
		currPhaseStepBeginTime = System.currentTimeMillis();
		currPhaseStepEndTime = -1;
	}

	public TimestampedNumber createRandomNumberBlocking(TimestampedNumber tsNum)
			throws InterruptedException {

		long now = System.currentTimeMillis();
		
		TimestampedNumber ret = null;
		
		if (now < currPhaseStepEndTime) {
			ret = nextTimestampedNumberBlocking(now, tsNum);
		} else {
			if (currPhase == LoadGenPhase.INITIAL_SLEEP) {
				Thread.sleep(5000);
				now = System.currentTimeMillis();
				// now in warmup phase
				transitionToNextPhase(now);
				ret = nextTimestampedNumberBlocking(now, tsNum);
			} else if (currPhase == LoadGenPhase.DONE) {
				Thread.sleep(100);
			} else {
				logStepStats(now);
				transitionToNextPhase(now);
				ret = nextTimestampedNumberBlocking(now, tsNum);
			}
		}
		
		return ret;
	}

	private void transitionToNextPhase(long now) throws InterruptedException {
		switch (currPhase) {
		case INITIAL_SLEEP:
			initWarmupPhase(System.currentTimeMillis());
			break;
		case WARMUP:
			initIncrementPhase(now);
			break;
		case INCREMENT:
			currPhaseStep++;
			if(currPhaseStep < currPhaseTotalSteps) {
				initCurrIncrementStep(now);
			} else {
				initPlateauPhase(now);
			}
			break;
		case PLATEAU:
			initDecrementPhase(now);
			break;
		case DECREMENT:
			currPhaseStep++;
			if(currPhaseStep < currPhaseTotalSteps) {
				initCurrDecrementStep(now);
			} else {
				initCooldownPhase(now);
			}
			break;
		case COOLDOWN:
			initDonePhase(now);
			break;
		case DONE:
			break;
		default:
			throw new RuntimeException("This should never happen");
		}		
	}

	private void initDonePhase(long now) {
		currPhase = LoadGenPhase.DONE;
		currPhaseTotalSteps = -1;
		currPhaseStep = -1;
		
		currPhaseStepBeginTime = now;
		currPhaseStepEndTime = -1;
	}

	private void initCooldownPhase(long now) {
		currPhase = LoadGenPhase.COOLDOWN;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(now, profile.finalPhaseDurationMillis,
				profile.minEmitsPerSecond);
	}

	private void initDecrementPhase(long now) {
		currPhase = LoadGenPhase.DECREMENT;
		currPhaseTotalSteps = profile.decrementPhaseSteps;
		currPhaseStep = 0;
		initCurrDecrementStep(now);		
	}

	private void initCurrDecrementStep(long now) {
		long stepDuration = profile.decrementPhaseDurationMillis
				/ currPhaseTotalSteps;
		
		int stepEmitsPerSecond = profile.maxEmitsPerSecond
				- (int) Math.round(currPhaseStep
								* (profile.maxEmitsPerSecond - profile.minEmitsPerSecond)
								/ ((double) currPhaseTotalSteps));
		
		configurePhaseStep(now, stepDuration, stepEmitsPerSecond);		
	}

	private void initPlateauPhase(long now) {
		currPhase = LoadGenPhase.PLATEAU;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(now, profile.plateauPhaseDurationMillis,
				profile.maxEmitsPerSecond);
		
	}

	private void configurePhaseStep(long now, long stepDurationMillies,
			int emitsPerSecond) {

		currPhaseStepBeginTime = now;
		currPhaseStepDuration = stepDurationMillies;
		currPhaseStepEndTime = now + stepDurationMillies;
		currPhaseStepTotalEmits = (int) Math.round(emitsPerSecond
				* (stepDurationMillies / 1000.0));
		currPhaseStepEmits = 0;
		sleepTime = 0;
		
		LOG.info(String.format("%s (step %d): Emitting %d recs/sec for %.1f sec",
				currPhase.toString(),
				currPhaseStep + 1,
				emitsPerSecond, stepDurationMillies / 1000.0));
		
	}

	private void initWarmupPhase(long now) {
		currPhase = LoadGenPhase.WARMUP;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(now, profile.warmupPhaseDurationMillis,
				profile.minEmitsPerSecond);
	}

	private void initIncrementPhase(long now) {
		currPhase = LoadGenPhase.INCREMENT;
		currPhaseTotalSteps = profile.incrementPhaseSteps;
		currPhaseStep = 0;
		initCurrIncrementStep(now);
	}

	private void initCurrIncrementStep(long now) {
		long stepDuration = profile.incrementPhaseDurationMillis
				/ currPhaseTotalSteps;
		
		int stepEmitsPerSecond = profile.minEmitsPerSecond
				+ (int) Math.round((currPhaseStep + 1)
								* (profile.maxEmitsPerSecond - profile.minEmitsPerSecond)
								/ ((double) currPhaseTotalSteps));
		
		configurePhaseStep(now, stepDuration, stepEmitsPerSecond);
	}

	private TimestampedNumber nextTimestampedNumberBlocking(long now,
			TimestampedNumber tsNum) throws InterruptedException {

		if (currPhaseStepEmits % 3 == 0) {
			adjustSleepTime(now);
		}
		currPhaseStepEmits++;

		if (sleepTime > 0) {
			Thread.sleep(sleepTime);
			now = System.currentTimeMillis();
		}

		byte[] nextNumBytes = new byte[32];
		rnd.nextBytes(nextNumBytes);
		BigInteger num = new BigInteger(nextNumBytes).abs();

		tsNum.timestamp = now;
		tsNum.number = num;
		return tsNum;
	}

	private void adjustSleepTime(long now) {
		int expectedEmitted = (int) (currPhaseStepTotalEmits * ((now - currPhaseStepBeginTime) / ((double) currPhaseStepDuration)));
		if (currPhaseStepEmits > expectedEmitted) {
			sleepTime++;
		} else if (currPhaseStepEmits < expectedEmitted && sleepTime > 0) {
			sleepTime--;
		}
	}

	private void logStepStats(long now) {
		double secsPassed = (now - currPhaseStepBeginTime) / 1000.0;
		int attemptedEmitsPerSecond = (int) (currPhaseStepTotalEmits / ((currPhaseStepEndTime - currPhaseStepBeginTime) / 1000.0));
		double actualEmitsPerSecond = currPhaseStepEmits / secsPassed;

		LOG.info(String
				.format("%s (step %d): Emitted %.1f recs/sec for %.1f sec (%d records total)",
						currPhase.toString(),
						currPhaseStep + 1,
						actualEmitsPerSecond, 
						secsPassed,
						currPhaseStepEmits));

		LOG.info(String.format("qb: %d;%d;%d", currPhaseStepBeginTime / 1000,
				attemptedEmitsPerSecond, (int) actualEmitsPerSecond));
	}
}
