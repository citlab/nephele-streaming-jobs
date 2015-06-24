package de.tuberlin.cit.livescale.job.task;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.record.VideoFrame;
import de.tuberlin.cit.livescale.job.task.channelselectors.GroupVideoFrameChannelSelector;
import de.tuberlin.cit.livescale.job.util.decoder.VideoDecoder;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.util.HashMap;
import java.util.Map;

public final class DecoderTask extends IocTask {

	private final GroupVideoFrameChannelSelector channelSelector = new GroupVideoFrameChannelSelector();
	private final Map<Long, VideoDecoder> streamId2Decoder = new HashMap<Long, VideoDecoder>();

	@Override
	public void setup() {
		initReader(0, Packet.class);
		initWriter(0, VideoFrame.class, channelSelector);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void decode(Packet packet, Collector<VideoFrame> out) {
		try {
			VideoDecoder decoder = streamId2Decoder.get(packet.getStreamId());
			if (decoder == null) {
				decoder = new VideoDecoder(packet.getStreamId(),
						packet.getGroupId());
				streamId2Decoder.put(packet.getStreamId(), decoder);
			}

			VideoFrame frameToEmit;
			if (!packet.isEndOfStreamPacket()) {
				frameToEmit = decoder.decodePacket(packet);
			} else {
				frameToEmit = decoder.createEndOfStreamFrame();
				decoder.closeDecoder();
				streamId2Decoder.remove(packet.getStreamId());
			}

			if (frameToEmit != null) {
				out.collect(frameToEmit);
				
				if (frameToEmit.isEndOfStreamFrame()) {
					out.flush();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void shutdown() {
		for (final VideoDecoder decoder : streamId2Decoder.values()) {
			decoder.closeDecoder();
		}
		streamId2Decoder.clear();
	}
}
