package de.tuberlin.cit.livescale.job.util.receiver;

import java.io.IOException;
import java.nio.ByteBuffer;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.util.encoder.ServerPortManager;
import de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp.BufferedFlvFile;
import de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp.FlvFileHeader;
import de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp.FlvStreamForwarderState;
import de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp.FlvStreamForwarderThread;
import de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp.FlvTag;

public class FlvOverTcpForwardingReceiver implements VideoReceiver {

	// private static final Log LOG = LogFactory.getLog(FlvOverTcpForwardingReceiver.class);

	private static final ServerPortManager PORT_MANAGER = new ServerPortManager(43100, 43110);

	private FlvStreamForwarderState currentState;

	private FlvTag currentTag;

	private final int TAG_BUFFER_SIZE = 100 * 1024;

	private BufferedFlvFile bufferedFile;

	private FlvStreamForwarderThread forwarderThread;

	public FlvOverTcpForwardingReceiver(long groupId) throws Exception {
		this.currentState = FlvStreamForwarderState.EMPTY;
		this.bufferedFile = new BufferedFlvFile();
		this.forwarderThread = new FlvStreamForwarderThread(groupId, bufferedFile, PORT_MANAGER);
		this.forwarderThread.start();
	}

	@Override
	public void writePacket(Packet packet) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(packet.getData());

		while (buffer.hasRemaining()) {
			switch (currentState) {
			case EMPTY:
				consumeFlvHeader(buffer);
				break;
			case FOUND_FLV_HEADER:
				consumeFlvMetaTag(buffer);
				break;
			case FOUND_FLV_META_TAG:
				consumeAvcConfigVideoTag(buffer);
				break;
			case BUFFERING_VIDEOTAGS:
				bufferVideoTags(buffer);
				break;
			}
		}
	}

	private void bufferVideoTags(ByteBuffer buffer) throws IOException {
		if (!currentTag.isFull()) {
			currentTag.consume(buffer);
		}

		if (currentTag.isFull()) {
			if (currentTag.isVideoTag()) {
				bufferedFile.enqueueFlvTag(currentTag);
				currentTag = new FlvTag(TAG_BUFFER_SIZE);
			} else {
				currentTag.reset();
			}
		}
	}

	private void consumeAvcConfigVideoTag(ByteBuffer buffer) throws IOException {
		FlvTag flvAvcConfigVideoTag = bufferedFile.getFlvAvcConfigVideoTag();
		if (flvAvcConfigVideoTag == null) {
			flvAvcConfigVideoTag = new FlvTag(1024);
			bufferedFile.setFlvAvcConfigVideoTag(flvAvcConfigVideoTag);
		}

		flvAvcConfigVideoTag.consume(buffer);

		if (flvAvcConfigVideoTag.isFull()) {
			currentState = FlvStreamForwarderState.BUFFERING_VIDEOTAGS;
			currentTag = new FlvTag(TAG_BUFFER_SIZE);
		}
	}

	private void consumeFlvMetaTag(ByteBuffer buffer) throws IOException {
		FlvTag flvMetaTag = bufferedFile.getFlvMetaTag();
		if (flvMetaTag == null) {
			flvMetaTag = new FlvTag(1024);
			bufferedFile.setFlvMetaTag(flvMetaTag);
		}

		flvMetaTag.consume(buffer);

		if (flvMetaTag.isFull()) {
			currentState = FlvStreamForwarderState.FOUND_FLV_META_TAG;
		}
	}

	private void consumeFlvHeader(ByteBuffer buffer) throws IOException {
		FlvFileHeader flvFileHeader = bufferedFile.getFlvFileHeader();
		if (flvFileHeader == null) {
			flvFileHeader = new FlvFileHeader();
			bufferedFile.setFlvFileHeader(flvFileHeader);
		}

		flvFileHeader.consume(buffer);

		if (flvFileHeader.isFull()) {
			currentState = FlvStreamForwarderState.FOUND_FLV_HEADER;
		}
	}

	@Override
	public void closeSafely() {
		forwarderThread.interrupt();
		try {
			forwarderThread.join();
		} catch (InterruptedException e) {
		}
	}

	@Override
	public long getGroupId() {
		return forwarderThread.getGroupId();
	}

	@Override
	public String getReceiveEndpointURL() {
		return forwarderThread.getReceiveEndpointURL();
	}
}
