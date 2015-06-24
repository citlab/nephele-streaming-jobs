package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface FlvFileAtom {

	public int consume(ByteBuffer data) throws IOException;

	public boolean isFull();

	public ByteBuffer getBufferForReading();

}
