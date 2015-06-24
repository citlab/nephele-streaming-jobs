package de.tuberlin.cit.livescale.job.util.receiver.udpmulticast;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.LinkedList;

import de.tuberlin.cit.livescale.job.record.Packet;

public class UdpClient {
	// private static Log LOG = LogFactory.getLog(UdpClient.class);

	private LinkedList<Packet> packetsToSend;

	private SocketAddress socketAddress;

	private ByteBuffer writeBuffer;

	private final int MAX_DATAGRAM_SIZE = 1400;

	private ByteBuffer datagramBuffer = ByteBuffer.allocate(MAX_DATAGRAM_SIZE);

	private int packetNo = 0;

	private int datagramNoInPacket = 0;

	public UdpClient(SocketAddress socketAddress) {
		this.packetsToSend = new LinkedList<Packet>();
		this.socketAddress = socketAddress;
	}

	public void enqueuePacket(Packet packet) {
		packetsToSend.add(packet);
		if (writeBuffer == null) {
			dequeuePacket();
		}
	}

	/**
	 * @param channel
	 * @return Whether the packet has been sent completely or not.
	 * @throws IOException
	 */
	public boolean sendNextPacket(DatagramChannel channel) throws IOException {
		if (!writeBuffer.hasRemaining()) {
			dequeuePacket();
		}

		if (writeBuffer.remaining() > MAX_DATAGRAM_SIZE - 2) {
			writeBuffer.limit(writeBuffer.position() + MAX_DATAGRAM_SIZE - 2);
		}

		datagramBuffer.clear();
		datagramBuffer.put((byte) packetNo);
		datagramBuffer.put((byte) datagramNoInPacket);
		datagramBuffer.put(writeBuffer);
		datagramBuffer.flip();

		channel.send(datagramBuffer, socketAddress);

		datagramNoInPacket++;

		// LOG.info("Sent " + datagram + " size " + (toSend.limit() - toSend.remaining()));

		writeBuffer.position(writeBuffer.position() - datagramBuffer.remaining());
		writeBuffer.limit(writeBuffer.capacity());

		if (writeBuffer.hasRemaining()) {
			return false;
		} else {
			return true;
		}
	}

	int counter = 0;

	private void dequeuePacket() {
		if (!packetsToSend.isEmpty()) {
			Packet packet = packetsToSend.removeFirst();
			writeBuffer = ByteBuffer.wrap(packet.getData());

			packetNo = (packetNo + 1) % 256;
			int estimatedNoOfDatagrams = (int) Math.ceil(writeBuffer.capacity() / (MAX_DATAGRAM_SIZE - 2.0));
			datagramNoInPacket = (byte) (estimatedNoOfDatagrams << 4);
		}
	}

	public SocketAddress getSocketAddress() {
		return socketAddress;
	}
}
