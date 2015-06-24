package de.tuberlin.cit.livescale.job.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

public class Packet extends AbstractTaggableRecord {

	private long streamId;

	private long groupId;

	private int packetIdInStream;

	private byte[] data;

	private long timestamp;

	/**
	 * Constructor used when reading a new Packet from a channel.
	 */
	public Packet() {
		this.streamId = -1;
		this.groupId = -1;
		this.packetIdInStream = -2; // dummy packet
		this.timestamp = -1;
	}

	/**
	 * Constructor to be used when creating a packet in order to write it to a channel.
	 * If {@link #packetIdInStream} is -1, then this packet becomes an empty "marker packet"
	 * that signals the end of the stream. This is meant to signal that any resources allocated for this stream
	 * should be released.
	 * 
	 * @param streamId
	 */
	public Packet(long streamId, long groupId, int packetIdInStream, byte[] data, long timestamp) {
		this.streamId = streamId;
		this.groupId = groupId;
		this.packetIdInStream = packetIdInStream;
		this.data = data;
		this.timestamp = timestamp;
	}

	public void set(int streamId, long groupId, int packetIdInStream, byte[] data, int offset, int len, int timestamp) {
		this.groupId = groupId;
		this.streamId = streamId;
		this.packetIdInStream = packetIdInStream;
		this.timestamp = timestamp;

		if (this.data == null) {
			this.data = new byte[len];
		}

		if (this.data.length != len) {
			this.data = new byte[len];
		}

		System.arraycopy(data, offset, this.data, 0, len);

		// Clear out the tag
		super.setTag(null);
	}

	public long getStreamId() {
		return streamId;
	}

	public byte[] getData() {
		return data;
	}

	public int getPacketIdInStream() {
		return packetIdInStream;
	}

	public boolean isEndOfStreamPacket() {
		return packetIdInStream == -1;
	}

	public void markAsEndOfStreamPacket() {
		this.packetIdInStream = -1;
		this.data = null;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);

		out.writeLong(streamId);
		out.writeLong(groupId);
		out.writeLong(timestamp);
		out.writeInt(packetIdInStream);

		if (!isEndOfStreamPacket() && !isDummyPacket()) {
			out.writeInt(data.length);
			out.write(data);
		}
	}

	public boolean isDummyPacket() {
		return this.packetIdInStream == -2;
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		streamId = in.readLong();
		groupId = in.readLong();
		timestamp = in.readLong();
		packetIdInStream = in.readInt();

		if (!isEndOfStreamPacket() && !isDummyPacket()) {
			int dataSize = in.readInt();
			this.data = new byte[dataSize];
			in.readFully(this.data);
		}
	}

	public long getGroupId() {
		return groupId;
	}

	public void setPacketIdInStream(int packetIdInStream) {
		this.packetIdInStream = packetIdInStream;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
