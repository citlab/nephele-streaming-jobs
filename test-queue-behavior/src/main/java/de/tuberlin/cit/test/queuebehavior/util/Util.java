package de.tuberlin.cit.test.queuebehavior.util;

/**
 * Created by bjoern on 12/15/14.
 */
public class Util {

	public static long alignToInterval(long timestampInMillis, long interval) {
		long remainder = timestampInMillis % interval;

		return timestampInMillis - remainder;
	}
}
