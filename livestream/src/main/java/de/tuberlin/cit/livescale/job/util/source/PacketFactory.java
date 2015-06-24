package de.tuberlin.cit.livescale.job.util.source;

import java.nio.ByteBuffer;

import de.tuberlin.cit.livescale.job.record.Packet;

public class PacketFactory {

	private long streamId;

	private long groupId;

	private int nextPacketIdInStream;

	private int packetSize;

	private ByteBuffer buffer;

	/**
	 * The state is an internal attribute used when creating packets from incoming
	 * streamed data. When the state indicates completeness of a packet,
	 * #{@link processDataInBuffer} returns a new packet.
	 */
	private PacketState state;

	public PacketFactory(long streamId, long groupId) {
		this.streamId = streamId;
		this.groupId = groupId;
		this.nextPacketIdInStream = -1;
		prepareNextPacket();
	}

	private void prepareNextPacket() {
		this.nextPacketIdInStream++;
		this.buffer = ByteBuffer.allocate(4);
		this.state = PacketState.SIZE_INCOMPLETE;
	}

	public Packet processDataInBuffer(ByteBuffer inBuffer) {
		int amountProcessed = 0;

		if (state == PacketState.SIZE_INCOMPLETE) {
			amountProcessed = tryToCompletePacketSize(inBuffer);
		}

		if (state == PacketState.PACKET_INCOMPLETE) {
			amountProcessed += tryToCompletePacket(inBuffer);
		}

		Packet toReturn = null;
		if (state == PacketState.COMPLETE) {
			toReturn = new Packet(streamId, groupId, nextPacketIdInStream, buffer.array(), System.currentTimeMillis());
			prepareNextPacket();
		}

		return toReturn;
	}

	private int tryToCompletePacket(ByteBuffer inBuffer) {
		int copied = safeBufferCopy(inBuffer, buffer);

		if (!buffer.hasRemaining()) {
			this.buffer.clear();
			this.state = PacketState.COMPLETE;
		}

		return copied;
	}

	private int safeBufferCopy(ByteBuffer from, ByteBuffer to) {
		byte[] targetArray = to.array();
		int amountToTransfer = Math.min(from.remaining(), to.remaining());
		from.get(targetArray, to.position(), amountToTransfer);
		to.position(to.position() + amountToTransfer);
		return amountToTransfer;
	}

	private int tryToCompletePacketSize(ByteBuffer inBuffer) {
		int copied = safeBufferCopy(inBuffer, buffer);

		if (!buffer.hasRemaining()) {
			this.buffer.clear();
			this.packetSize = buffer.getInt();
			byte[] bufferArray = new byte[packetSize];
			this.buffer = ByteBuffer.wrap(bufferArray);
			this.state = PacketState.PACKET_INCOMPLETE;
		}

		return copied;
	}

	public boolean isComplete() {
		return state == PacketState.COMPLETE;
	}
}
