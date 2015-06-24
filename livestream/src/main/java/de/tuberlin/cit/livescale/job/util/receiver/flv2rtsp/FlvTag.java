package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlvTag implements FlvFileAtom {

	private static final Log LOG = LogFactory.getLog(FlvTag.class);

	private static final int TAG_HEADER_LENGTH = 11;

	private static final int PREVIOUS_TAG_SIZE_LENGTH = 4;

	private ByteBuffer buffer;

	private int tagBodyLength;

	private volatile FlvTagState currentState;

	private FlvTagType type;

	private long timestamp;

	public FlvTag(ByteBuffer buffer) {
		this.buffer = buffer;
		reset();
	}

	public void reset() {
		this.buffer.clear();
		this.currentState = FlvTagState.EMPTY;
		this.tagBodyLength = 0;
		this.timestamp = -1;
		this.type = null;
	}

	public FlvTag(int bufferSize) {
		this(ByteBuffer.allocate(bufferSize));
	}

	@Override
	public int consume(ByteBuffer data) throws IOException {
		int bytesRead = 0;

		while (currentState != FlvTagState.FULL && data.hasRemaining()) {
			switch (currentState) {
			case EMPTY:
				bytesRead += consumeTagHeader(data);
				break;
			case FOUND_HEADER:
				bytesRead += consumeTagBody(data);
				break;
			case FOUND_BODY:
				bytesRead += consumePreviousTagSize(data);
				break;
			case FULL:
				throw new IOException("Flv tag is already full.");
			}
		}

		return bytesRead;
	}

	private int consumePreviousTagSize(ByteBuffer data) {
		int remainingPreviousTagSizeToRead = PREVIOUS_TAG_SIZE_LENGTH
			- (buffer.position() - TAG_HEADER_LENGTH - tagBodyLength);

		int toRead = Math.min(remainingPreviousTagSizeToRead, data.remaining());
		transferToBuffer(data, toRead);

		if (buffer.position() == TAG_HEADER_LENGTH + tagBodyLength + PREVIOUS_TAG_SIZE_LENGTH) {
			this.currentState = FlvTagState.FULL;
		}

		return toRead;
	}

	private int consumeTagBody(ByteBuffer data) {
		int remainingBodyToRead = tagBodyLength - (buffer.position() - TAG_HEADER_LENGTH);

		int toRead = Math.min(remainingBodyToRead, data.remaining());
		transferToBuffer(data, toRead);

		if (buffer.position() == TAG_HEADER_LENGTH + tagBodyLength) {
			this.currentState = FlvTagState.FOUND_BODY;
			// LOG.info(isVideoKeyframe());
		}

		return toRead;
	}

	private void transferToBuffer(ByteBuffer data, int toRead) {
		if (buffer.remaining() < toRead) {
			System.out.println("overflow: have to read " + toRead + " but only " + buffer.remaining()
				+ " remain in buffer.");
		}
		buffer.put(data.array(), data.position(), toRead);
		data.position(data.position() + toRead);
	}

	private int consumeTagHeader(ByteBuffer data) throws IOException {
		int remainingHeaderToRead = TAG_HEADER_LENGTH - buffer.position();

		int toRead = Math.min(remainingHeaderToRead, data.remaining());
		transferToBuffer(data, toRead);

		if (buffer.position() == TAG_HEADER_LENGTH) {
			this.currentState = FlvTagState.FOUND_HEADER;
			// body length is a uint24 (big endian) starting at index 1
			// to avoid clumsy uint24 parsing we read uint32 and mask out
			// the first byte
			this.tagBodyLength = buffer.getInt(0) & 0xFFFFFF;
			this.type = parseTagType();
			this.timestamp = parseTimestamp();
			increaseBufferSizeIfNecessary();
		}

		return toRead;
	}

	private void increaseBufferSizeIfNecessary() {
		int requiredBufferSize = TAG_HEADER_LENGTH + tagBodyLength + PREVIOUS_TAG_SIZE_LENGTH;
		if (requiredBufferSize > buffer.capacity()) {
			LOG.warn(String.format("Initial flv tag buffer size of %d is too small. Increasing to %d",
				buffer.capacity(),
				requiredBufferSize));

			ByteBuffer newBuffer = ByteBuffer.allocate(requiredBufferSize);
			newBuffer.put(buffer.array(), 0, buffer.position());
			this.buffer = newBuffer;
		}
	}

	private long parseTimestamp() {
		byte[] reordered = new byte[8];
		int offset = 4;
		reordered[4] = buffer.get(offset + 3);
		reordered[5] = buffer.get(offset + 0);
		reordered[6] = buffer.get(offset + 1);
		reordered[7] = buffer.get(offset + 2);

		return ByteBuffer.wrap(reordered).getLong();
	}

	@Override
	public boolean isFull() {
		return currentState == FlvTagState.FULL;
	}

	public FlvTagType getType() {
		return this.type;
	}

	public boolean isVideoKeyframe() {
		boolean isVideo = (type == FlvTagType.VIDEO);
		boolean isKeyframe = (buffer.get(TAG_HEADER_LENGTH) >>> 4) == 1;
		return isVideo && isKeyframe;
	}

	public boolean isVideoInterframe() {
		boolean isVideo = (type == FlvTagType.VIDEO);

		int frameType = buffer.get(TAG_HEADER_LENGTH) >>> 4;
		boolean isInterframe = (frameType == 2) || (frameType == 3);
		return isVideo && isInterframe;
	}

	private FlvTagType parseTagType() {
		byte typeByte = (byte) (buffer.get(0) & 0x1F);

		if (typeByte == 0x08) {
			return FlvTagType.AUDIO;
		} else if (typeByte == 0x09) {
			return FlvTagType.VIDEO;
		} else if (typeByte == 0x12) {
			return FlvTagType.META;
		} else {
			LOG.warn("Unknown tag type " + typeByte);
			return FlvTagType.UNKNOWN;
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public ByteBuffer getBufferForReading() {
		if (!isFull()) {
			throw new RuntimeException("Cannot return buffer of non-full video tag for reading");
		}

		buffer.position(0);
		buffer.limit(TAG_HEADER_LENGTH + tagBodyLength + PREVIOUS_TAG_SIZE_LENGTH);
		return buffer;
	}

	public void rebaseTimestamp(long videostreamTimestampOffset) {
		long rebasedTimestamp = timestamp - videostreamTimestampOffset;

		int offset = 4;
		// frame time stamp
		buffer.put(offset + 0, (byte) ((rebasedTimestamp >> 16) & 0xff));
		buffer.put(offset + 1, (byte) ((rebasedTimestamp >> 8) & 0xff));
		buffer.put(offset + 2, (byte) (rebasedTimestamp & 0xff));

		// frame time stamp extend
		buffer.put(offset + 3, (byte) ((rebasedTimestamp >> 24) & 0xff));
	}

	public boolean isVideoTag() {
		return type == FlvTagType.VIDEO;
	}
}
