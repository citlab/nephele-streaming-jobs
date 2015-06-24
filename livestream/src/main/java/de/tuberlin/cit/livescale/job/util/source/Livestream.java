package de.tuberlin.cit.livescale.job.util.source;

import java.nio.ByteBuffer;

public class Livestream {

	private long streamStartTimeInMillis;

	private byte[] data;

	private int packetIdInStream;

	/**
	 * If the video file is packetized, then this points to the first byte of the header of the current packet. The
	 * header is always 4 bytes long and contains the payload length as uint32. If this video file is NOT
	 * packetized, then this points to the first byte of FLV data.
	 */
	private int packetBegin;

	/**
	 * The length of the current packet (not including the uint32 header in packetized format).
	 */
	private int packetSize;

	/**
	 * The time in millis since the epoch when the current packet should be written.
	 */
	private long packetTimeOfWrite;

	private boolean eofReached;

	private boolean isPacketized;

	private boolean producePacketizedStream;

	public Livestream(byte[] data, boolean dataIsPacketized, long startTimeInMillis, boolean producePacketizedStream) {
		this.data = data;
		this.isPacketized = dataIsPacketized;
		this.eofReached = false;
		this.packetBegin = 0;
		this.streamStartTimeInMillis = startTimeInMillis;
		this.packetIdInStream = 0;
		this.producePacketizedStream = producePacketizedStream;

		if (!dataIsPacketized && producePacketizedStream) {
			throw new RuntimeException(
				"Producing packetized streams from non-packetized data is currently not implemented");
		}

		determinePacketLength();
		determineCurrentPacketTimeOfWrite();
	}

	private void determinePacketLength() {
		if (isPacketized) {
			ByteBuffer buffer = ByteBuffer.wrap(data, packetBegin, 4);
			this.packetSize = buffer.getInt();
		} else {
			if (packetBegin == 0) {
				// flv_header_size + previous_tag_length_size = 9 + 4 = 13
				this.packetSize = 13;
			} else {
				// flv_tag_body_size is a uint24_be starting at second byte
				// of flv_tag_header
				byte[] bodySizeBuffer = new byte[] { 0, 0, 0, 0 };
				System.arraycopy(data, packetBegin + 1, bodySizeBuffer, 1, 3);

				// flv_tag_header_size + flv_tag_body_size + previous_tag_length_size
				this.packetSize = 11 + ByteBuffer.wrap(bodySizeBuffer).getInt() + 4;
			}
		}
	}

	public void shiftToNextPacket() {
		if (!eofReached) {
			if (isPacketized) {
				packetBegin += 4 + packetSize;
			} else {
				packetBegin += packetSize;
			}

			// make sure we can read packet size, packet timestamp
			// and at least one byte of package data
			eofReached = packetBegin + 12 > data.length;
			if (!eofReached) {
				determinePacketLength();
				determineCurrentPacketTimeOfWrite();
				packetIdInStream++;
				eofReached = packetBegin + 4 + packetSize > data.length;
			}
		}
	}

	public boolean isEOF() {
		return eofReached;
	}

	public void fillBufferWithCurrentPacket(ByteBuffer buffer) {
		buffer.clear();
		if (isPacketized && producePacketizedStream) {
			buffer.put(data, packetBegin, packetSize + 4);
		} else if (isPacketized && !producePacketizedStream) {
			buffer.put(data, packetBegin + 4, packetSize);
		} else {
			buffer.put(data, packetBegin, packetSize);
		}
		buffer.flip();
	}

	public void fillArrayWithCurrentPacketPayload(byte[] array, int offset) {
		if (isPacketized && producePacketizedStream) {
			System.arraycopy(data, packetBegin, array, offset, packetSize + 4);
		} else if (isPacketized && !producePacketizedStream) {
			System.arraycopy(data, packetBegin + 4, array, offset, packetSize);
		} else {
			System.arraycopy(data, packetBegin, array, offset, packetSize);
		}
	}

	public long getTimeOfWriteForCurrentPacket() {
		return this.packetTimeOfWrite;
	}

	public int getPacketSizeForCurrentPacket() {
		return this.packetSize;
	}

	public int getPacketIdInStreamForCurrentPacket() {
		return packetIdInStream;
	}

	private void determineCurrentPacketTimeOfWrite() {
		if (packetBegin > 0) {

			int offset;
			if (isPacketized) {
				// packet_length(4) + flv_tag_type(1) + flv_tag_body_length(3)
				offset = packetBegin + 4 + 1 + 3;
			} else {
				// flv_tag_type(1) + flv_tag_body_length(3)
				offset = packetBegin + 1 + 3;
			}

			byte[] reordered = new byte[8];
			reordered[4] = data[offset + 3];
			reordered[5] = data[offset + 0];
			reordered[6] = data[offset + 1];
			reordered[7] = data[offset + 2];

			long streamTimestamp = ByteBuffer.wrap(reordered).getLong();
			this.packetTimeOfWrite = streamStartTimeInMillis + streamTimestamp;
		} else {
			this.packetTimeOfWrite = streamStartTimeInMillis;
		}
	}

	public void rewind(long startTimeInMillis) {
		this.eofReached = false;
		this.packetBegin = 0;
		this.streamStartTimeInMillis = startTimeInMillis;
		this.packetIdInStream = 0;

		determinePacketLength();
		determineCurrentPacketTimeOfWrite();
	}
}
