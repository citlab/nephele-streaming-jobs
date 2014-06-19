package de.tuberlin.cit.test.queuebehavior;

import java.util.HashMap;

public class TestQueueBehaviorJobProfile {

	/**
	 * Degree of parallelism of inner tasks (decoder, merger, overlay, encoder)
	 */
	public final int innerTaskDop;

	/**
	 * Number of inner tasks per instance.
	 */
	public final int innerTaskDopPerInstance;

	/**
	 * Degree of parallelism for outer tasks (source, sink).
	 */
	public final int outerTaskDop;

	/**
	 * Number of outer tasks per instance.
	 */
	public final int outerTaskDopPerInstance;
	
	public final static HashMap<String, TestQueueBehaviorJobProfile> PROFILES = new HashMap<>();

	public final static TestQueueBehaviorJobProfile WALLY200 = new TestQueueBehaviorJobProfile(
			"wally200", 800, 4, 200, 1);

	public final static TestQueueBehaviorJobProfile WALLY199 = new TestQueueBehaviorJobProfile(
			"wally199", 796, 4, 199, 1);
	
	public final static TestQueueBehaviorJobProfile WALLY50 = new TestQueueBehaviorJobProfile(
			"wally50", 200, 4, 50, 1);

	public final static TestQueueBehaviorJobProfile LOCAL_DUALCORE = new TestQueueBehaviorJobProfile(
			"local_dualcore", 2, 2, 1, 1);

	public TestQueueBehaviorJobProfile(String name, int innerTaskDop,
			int innerTaskDopPerInstance, int outerTaskDop,
			int outerTaskDopPerInstance) {

		this.innerTaskDop = innerTaskDop;
		this.innerTaskDopPerInstance = innerTaskDopPerInstance;
		this.outerTaskDop = outerTaskDop;
		this.outerTaskDopPerInstance = outerTaskDopPerInstance;

		if (PROFILES.containsKey(name)) {
			throw new IllegalArgumentException("Profile name " + name
					+ " already reserved");
		}
		PROFILES.put(name, this);
	}
}
