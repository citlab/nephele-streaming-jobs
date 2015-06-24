package de.tuberlin.cit.livescale.job.util.receiver.tcpserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import de.tuberlin.cit.livescale.job.record.Packet;

public class TcpClient {
	// private static Log LOG = LogFactory.getLog(TcpClient.class);

	private LinkedList<Packet> packetsToSend;

	private ByteBuffer writeBuffer;

	private boolean clientIsReceiving;

	private final static int INITIAL_WRITE_BUFFER_SIZE = 100 * 1024;

	public TcpClient() {
		this.packetsToSend = new LinkedList<Packet>();
		this.writeBuffer = ByteBuffer.allocate(INITIAL_WRITE_BUFFER_SIZE);
		this.writeBuffer.limit(0);
		this.clientIsReceiving = false;
	}

	public void enqueuePacket(Packet packet) {
		packetsToSend.add(packet);
	}

	/**
	 * @param socketChannel
	 * @return Whether there still remains data to be sent
	 * @throws IOException
	 */
	public boolean sendPacketData(SocketChannel socketChannel) throws IOException {
		if (!writeBuffer.hasRemaining()) {
			dequeuePacket();
		}

		socketChannel.write(writeBuffer);

		if (writeBuffer.hasRemaining()) {
			return false;
		} else {
			return packetsToSend.isEmpty();
		}
	}

	private void dequeuePacket() {
		if (!packetsToSend.isEmpty()) {
			Packet packet = packetsToSend.removeFirst();
			writeBuffer.clear();
			ensureWriteBufferIsLargeEnough(packet);
			writeBuffer.put(packet.getData());
			writeBuffer.flip();
		}
	}

	private void ensureWriteBufferIsLargeEnough(Packet packet) {
		if (this.writeBuffer.capacity() < packet.getData().length) {
			this.writeBuffer = ByteBuffer.allocate(packet.getData().length);
		}
	}

	public void setReceiving(boolean clientIsReceiving) {
		this.clientIsReceiving = clientIsReceiving;
	}

	public boolean isReceiving() {
		return this.clientIsReceiving;
	}
}
