package de.tuberlin.cit.livescale.job.util.decoder;

import com.xuggle.xuggler.io.IURLProtocolHandler;

public class ByteBufferedURLProtocolHandler implements IURLProtocolHandler {

//	private static Log LOG = LogFactory.getLog(ByteBufferedURLProtocolHandler.class);

	private IOBuffer ringBuffer;

	public ByteBufferedURLProtocolHandler(String name) {
		this.ringBuffer = new IOBuffer(name);
	}

	public ByteBufferedURLProtocolHandler(int bufferSize, String name) {
		this.ringBuffer = new IOBuffer(bufferSize, name);
	}

	@Override
	public int open(String url, int flags) {
//		LOG.warn(String.format("%s: open(%s, %d)", name, url, flags));
		return 0;
	}

	@Override
	public int close() {
//		LOG.warn(String.format("%s: close()", name));
		return 0;
	}

	public int availableForReading() {
		return ringBuffer.availableForReading();
	}

	@Override
	public int read(byte[] readBuffer, int size) {
		return ringBuffer.read(readBuffer, size);
	}

	@Override
	public int write(byte[] data, int size) {
		return ringBuffer.write(data, 0, size);
	}

	public int write(byte[] data, int offset, int amount) {
		return ringBuffer.write(data, offset, amount);
	}

	@Override
	public long seek(long offset, int whence) {
		return ringBuffer.seek(offset, whence);
	}

	@Override
	public boolean isStreamed(String url, int flags) {
//		LOG.warn(String.format("%s: isStreamed(%s, %d)", name, url, flags));
		return true;
	}

}
