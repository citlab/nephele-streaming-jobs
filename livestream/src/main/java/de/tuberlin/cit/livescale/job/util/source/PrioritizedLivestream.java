package de.tuberlin.cit.livescale.job.util.source;

public class PrioritizedLivestream extends Livestream implements
		Comparable<PrioritizedLivestream> {

	private long streamId;

	private int groupId;

	public PrioritizedLivestream(VideoFile videoFile,
			boolean videoFileIsPacketized, long streamId, int groupId,
			long startTime) {
		
		super(videoFile.getData(), videoFileIsPacketized, startTime, false);
		this.streamId = streamId;
		this.groupId = groupId;
	}

	@Override
	public int compareTo(PrioritizedLivestream other) {
		if (this.getTimeOfWriteForCurrentPacket() < other
				.getTimeOfWriteForCurrentPacket()) {
			return -1;
		} else if (this.getTimeOfWriteForCurrentPacket() > other
				.getTimeOfWriteForCurrentPacket()) {
			return 1;
		} else {
			return 0;
		}
	}

	public long getStreamId() {
		return streamId;
	}

	public int getGroupId() {
		return groupId;
	}
}
