package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile;
import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile.LoadGenerationProfile;
import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource.TimestampedNumber;
import eu.stratosphere.nephele.io.OpportunisticRoundRobinChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

public class NumberSourceTask extends AbstractGenericInputTask {

	public static final String PROFILE_PROPERTY_KEY = "TEST_QUEUE_JOB_PROFILE";
	
	public static final String PROFILE_PROPERTY_DEFAULT = TestQueueBehaviorJobProfile.LOCAL_DUALCORE.name;

	public static final String GLOBAL_BEGIN_TIME_PROPERTY_KEY = "GLOBAL_BEGIN_TIME";

	private RecordWriter<NumberRecord> out;

	private long globalBeginTime;

	@Override
	public void registerInputOutput() {
//		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class,
//				new OpportunisticRoundRobinChannelSelector<NumberRecord>(
//						getCurrentNumberOfSubtasks(), getIndexInSubtaskGroup()));
		
		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class);

		globalBeginTime = getTaskConfiguration().getLong(GLOBAL_BEGIN_TIME_PROPERTY_KEY, -1);
		if (globalBeginTime == -1) {
			throw new RuntimeException("No globalBeginTime provided");
		} else if (globalBeginTime < System.currentTimeMillis()) {
			throw new RuntimeException(String.format("globalBeginTime is in the past. %d", globalBeginTime));
		}
	}

	@Override
	public void invoke() throws Exception {
		LoadGenerationProfile profile = TestQueueBehaviorJobProfile.PROFILES
				.get(getTaskConfiguration().getString(PROFILE_PROPERTY_KEY,
						PROFILE_PROPERTY_DEFAULT)).loadGenProfile;

		BlockingRandomNumberSource rndSource = new BlockingRandomNumberSource(profile, globalBeginTime);
		TimestampedNumber numHolder = new TimestampedNumber();

		TimestampedNumber toEmit;
		while ((toEmit = rndSource.createRandomNumberBlocking(numHolder)) != null) {
			NumberRecord record = new NumberRecord();
			record.setNumber(toEmit.number);
			record.setTimestamp(toEmit.timestamp);
			out.emit(record);
		}
	}
}
