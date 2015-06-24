package de.tuberlin.cit.livescale.job.task.channelselectors;

import de.tuberlin.cit.livescale.job.record.Packet;
import eu.stratosphere.nephele.io.ChannelSelector;

public class StreamPacketChannelSelector implements ChannelSelector<Packet> {

	private final int[] channelIds = new int[1];

	@Override
	public int[] selectChannels(Packet packet, int numberOfOutputChannels) {
		if(packet.isDummyPacket()) {
			int[] all = new int[numberOfOutputChannels];
			for(int i=0; i<numberOfOutputChannels; i++) {
				all[i] = i;
			}
			return all;
		}

		this.channelIds[0] = (int) (packet.getStreamId() % numberOfOutputChannels);
		return this.channelIds;
	}
}
