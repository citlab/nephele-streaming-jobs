package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FlvFileHeader implements FlvFileAtom {
	// (9 bytes header, 4 bytes previous tag length)
	private static final int FLV_FILE_HEADER_SIZE = 13;

	private ByteBuffer buffer;

	private boolean isFull;

	public FlvFileHeader() {
		this.buffer = ByteBuffer.allocate(FLV_FILE_HEADER_SIZE);
		this.isFull = false;
	}

	@Override
	public int consume(ByteBuffer data) throws IOException {
		if (isFull) {
			throw new IOException("FLV file header has already been read");
		}

		int toRead = Math.min(buffer.remaining(), data.remaining());
		buffer.put(data.array(), data.position(), toRead);
		data.position(data.position() + toRead);

		if (!buffer.hasRemaining()) {
			checkFlvHeaderValidity();
			isFull = true;
		}

		return toRead;
	}

	@Override
	public boolean isFull() {
		return isFull;
	}

	private void checkFlvHeaderValidity() throws IOException {
		// 0x464c56 = FLV
		if (buffer.get(0) != (byte) 0x46
				|| buffer.get(1) != (byte) 0x4c
				|| buffer.get(2) != (byte) 0x56) {
			throw new IOException("Unsupported container format. This is not FLV.");
		}
	}

	@Override
	public ByteBuffer getBufferForReading() {
		if (!isFull()) {
			throw new RuntimeException("Cannot return buffer of non-full flv file header for reading");
		}

		buffer.position(0);
		buffer.limit(FLV_FILE_HEADER_SIZE);
		return buffer;
	}

}
