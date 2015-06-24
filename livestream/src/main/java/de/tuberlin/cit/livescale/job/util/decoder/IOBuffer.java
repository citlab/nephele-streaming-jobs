package de.tuberlin.cit.livescale.job.util.decoder;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xuggle.xuggler.io.IURLProtocolHandler;

public class IOBuffer {
	private static final int DEFAULT_BUFFER_SIZE = 512 * 1024;

	private static Log LOG = LogFactory.getLog(IOBuffer.class);

	/**
	 * Buffer to read to and write from. The ByteBuffer's position is points
	 * to where to read from. The ByteBuffer's limit points to where to write next.
	 */
	private ByteBuffer buffer;

	private int amountWritten;

	private String name;

	public IOBuffer(String name) {
		this.name = name;
		buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
		buffer.limit(0);
		amountWritten = 0;
	}

	public IOBuffer(int size, String name) {
		this.name = name;
		buffer = ByteBuffer.allocate(size);
		buffer.limit(0);
	}

	public int availableForReading() {
		return buffer.remaining();
	}

	public int read(byte[] readBuffer, int size) {
		// LOG.warn(String.format("%s: read(%d)", name, size));
		// some data available?
		if (buffer.hasRemaining()) {
			int toCopy = Math.min(buffer.remaining(), size);
			buffer.get(readBuffer, 0, toCopy);
			return toCopy;
		} else {
			LOG.warn(name + ": Ooops. Reached end of file.");
			return 0;
		}
	}

	public int write(byte[] data, int offset, int amount) {
		// LOG.warn(String.format("%s: write(%d)", name, amount));
		if (spaceLeftInBuffer() < amount) {
			rewindBuffer();
		}

		int amountToWrite = Math.min(amount, spaceLeftInBuffer());
		buffer.mark();
		buffer.position(buffer.limit());
		buffer.limit(buffer.limit() + amountToWrite);
		buffer.put(data, offset, amount);
		buffer.reset();

		if (amountToWrite < amount) {
			LOG.warn(name + ": Ooops. Buffer is full, could not write complete data.");
		}

		this.amountWritten += amountToWrite;

		return amountToWrite;
	}

	private void rewindBuffer() {
		int rewindableAmount = buffer.position();

		if (rewindableAmount > 0) {
			byte[] bufferArray = buffer.array();
			int bufferPosition = buffer.position();
			int bufferLimit = buffer.limit();
			System.arraycopy(bufferArray, buffer.position(), bufferArray, 0, bufferLimit - bufferPosition);
			buffer.position(0);
			buffer.limit(bufferLimit - rewindableAmount);
		}
	}

	private int spaceLeftInBuffer() {
		return buffer.capacity() - buffer.limit();
	}

	public long seek(long offset, int whence) {
		// if (whence != IURLProtocolHandler.SEEK_SIZE) {
		// String seekTypeString = (new String[] { "SEEK_SET", "SEEK_CUR", "SEEK_END" })[whence];
		// LOG.warn(String.format("%s: ffmpeg seeks %s %d", name, seekTypeString, offset));
		// }

		if (whence == IURLProtocolHandler.SEEK_SIZE) {
			return amountWritten;
		} else {
			return tryToSeek(offset, whence);
		}
	}

	private long tryToSeek(long offset, int whence) {

		int newBufferPosition = -1;

		switch (whence) {
		case IURLProtocolHandler.SEEK_CUR:
			newBufferPosition = (int) (buffer.position() + offset);
			break;
		case IURLProtocolHandler.SEEK_END:
			newBufferPosition = (int) (buffer.limit() + offset);
			break;
		case IURLProtocolHandler.SEEK_SET:
			newBufferPosition = (int) (buffer.limit() - (amountWritten - offset));
			break;
		}

		if (newBufferPosition >= 0 || newBufferPosition <= buffer.limit()) {
			buffer.position(newBufferPosition);
			return offset;
		} else {
			String seekTypeString = (new String[] { "SEEK_SET", "SEEK_CUR", "SEEK_END" })[whence];
			LOG.warn(String.format("%s: ffmpeg tried to use unsupported seek type %s %d", name, seekTypeString, offset));
			return -1;
		}
	}

}
