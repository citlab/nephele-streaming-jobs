package de.tuberlin.cit.livescale.job.util.source;

public class IdGenerator {

	private long firstId;

	private long nextId;

	private long lastId;

	public IdGenerator(int noOfIdSpaceSegments, int idSpaceSegmentIndex, int segmentSize) {
		int requiredBitsForSegmentIndex = (int) Math.ceil(Math.log(noOfIdSpaceSegments) / Math.log(Math.E));
		int requiredBitsForSegment = (int) Math.ceil(Math.log(segmentSize) / Math.log(Math.E));

		if (requiredBitsForSegmentIndex + requiredBitsForSegment > 64) {
			throw new RuntimeException(
				"Required amount of bits for ID space segment index and segment is larger than 64.");
		}

		this.firstId = ((long) idSpaceSegmentIndex) << requiredBitsForSegment;
		this.nextId = firstId;
		this.lastId = this.firstId + segmentSize - 1;
	}

	public long nextId() {
		long toReturn = nextId;

		nextId++;
		if (nextId > lastId) {
			nextId = firstId;
		}

		return toReturn;
	}
}
