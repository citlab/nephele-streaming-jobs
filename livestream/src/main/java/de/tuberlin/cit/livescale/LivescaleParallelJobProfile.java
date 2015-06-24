package de.tuberlin.cit.livescale;

import java.util.HashMap;

public class LivescaleParallelJobProfile {

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

	/**
	 * Number of concurrent video streams to generate.
	 */
	public final int noOfStreams;

	/**
	 * Defines the number of concurrent video streams that get merged into a new
	 * video stream.
	 */
	public final int noOfStreamsPerGroup;
	
	public final static HashMap<String, LivescaleParallelJobProfile> PROFILES = new HashMap<>();

	public final static LivescaleParallelJobProfile WALLY200 = new LivescaleParallelJobProfile(
			"wally200", 800, 4, 200, 1, 6400, 4);

	public final static LivescaleParallelJobProfile WALLY199 = new LivescaleParallelJobProfile(
			"wally199", 796, 4, 199, 1, 6368, 4);
	
	public final static LivescaleParallelJobProfile WALLY50 = new LivescaleParallelJobProfile(
			"wally50", 200, 4, 50, 1, 1600, 4);

	public final static LivescaleParallelJobProfile WALLY100 = new LivescaleParallelJobProfile(
					"wally100", 400, 4, 100, 1, 3200, 4);

	public final static LivescaleParallelJobProfile LOCAL_DUALCORE = new LivescaleParallelJobProfile(
			"local_dualcore", 2, 2, 1, 1, 8, 4);

	public LivescaleParallelJobProfile(String name, int innerTaskDop,
			int innerTaskDopPerInstance, int outerTaskDop,
			int outerTaskDopPerInstance, int noOfStreams,
			int noOfStreamsPerGroup) {

		this.innerTaskDop = innerTaskDop;
		this.innerTaskDopPerInstance = innerTaskDopPerInstance;
		this.outerTaskDop = outerTaskDop;
		this.outerTaskDopPerInstance = outerTaskDopPerInstance;
		this.noOfStreams = noOfStreams;
		this.noOfStreamsPerGroup = noOfStreamsPerGroup;

		if (PROFILES.containsKey(name)) {
			throw new IllegalArgumentException("Profile name " + name
					+ " already reserved");
		}
		PROFILES.put(name, this);
	}
}
