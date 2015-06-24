package de.tuberlin.cit.livescale.job.util.receiver;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import de.tuberlin.cit.livescale.job.record.Packet;

public class FileVideoReceiver implements VideoReceiver {

	private long streamId;

	private String filename;

	private BufferedOutputStream outputStream;

	private String receiveEndpointUrl;

	public FileVideoReceiver(long streamId, String filename) throws IOException {
		this.streamId = streamId;
		this.filename = filename;
		openOutputStream();
	}

	private void openOutputStream() throws IOException {
		File file = new File(filename);
		this.receiveEndpointUrl = String.format("file://%s", file.getAbsoluteFile());
		this.outputStream = new BufferedOutputStream(new FileOutputStream(filename));
	}

	public void writePacket(Packet packet) throws IOException {
		this.outputStream.write(packet.getData());
	}

	public void closeSafely() {
		try {
			this.outputStream.close();
		} catch (IOException e) {
		}
	}

	public long getGroupId() {
		return streamId;
	}

	@Override
	public String getReceiveEndpointURL() {
		return receiveEndpointUrl;
	}
}
