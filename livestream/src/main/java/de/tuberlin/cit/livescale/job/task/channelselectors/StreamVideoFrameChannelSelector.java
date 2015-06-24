package de.tuberlin.cit.livescale.job.task.channelselectors;

import de.tuberlin.cit.livescale.job.record.VideoFrame;
import eu.stratosphere.nephele.io.ChannelSelector;

public class StreamVideoFrameChannelSelector implements
		ChannelSelector<VideoFrame> {

	private final int[] channelIds = new int[1];

	@Override
	public int[] selectChannels(VideoFrame frame, int numberOfOutputChannels) {
		if (frame.isDummyFrame()) {
			int[] all = new int[numberOfOutputChannels];
			for (int i = 0; i < numberOfOutputChannels; i++) {
				all[i] = i;
			}
			return all;
		}

		this.channelIds[0] = (int) (frame.streamId % numberOfOutputChannels);
		return this.channelIds;
	}
}
