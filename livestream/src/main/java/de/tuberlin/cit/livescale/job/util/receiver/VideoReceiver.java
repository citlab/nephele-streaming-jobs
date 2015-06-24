package de.tuberlin.cit.livescale.job.util.receiver;

import java.io.IOException;

import de.tuberlin.cit.livescale.job.record.Packet;

public interface VideoReceiver {
	
	public void writePacket(Packet packet) throws IOException;
	
	public void closeSafely();
	
	public long getGroupId();

	public String getReceiveEndpointURL();
}
