package de.tuberlin.cit.livescale.job.util.source;

import java.nio.channels.SelectionKey;

public class Stream {
	private static final long STREAM_EXPIRY_THRESHOLD = 15000;

	private PacketFactory streamPacketFactory;

	private SelectionKey selectionKey;

	private long streamId;

	private long groupId;

	private String sendEndpointHost;

	private int sendEndpointPort;

	private String sendEndpointToken;

	private String receiveEndpointToken;

	private String receiveEndpointHost;

	private int receiveEndpointPort;

	private long creationTime;

	private StreamManager streamManager;

	public Stream(StreamManager streamManager, long streamId, long groupId) {
		this.streamManager = streamManager;
		this.creationTime = System.currentTimeMillis();
		this.streamId = streamId;
		this.groupId = groupId;
	}

	public PacketFactory getStreamPacketFactory() {
		return streamPacketFactory;
	}

	public void setStreamPacketFactory(PacketFactory streamPacketFactory) {
		this.streamPacketFactory = streamPacketFactory;
	}

	public SelectionKey getSelectionKey() {
		return selectionKey;
	}

	public void setSelectionKey(SelectionKey selectionKey) {
		this.selectionKey = selectionKey;
		this.streamManager.registerBySelectionKey(selectionKey, this);
	}

	public String getSendEndpointHost() {
		return sendEndpointHost;
	}

	public void setSendEndpointHost(String sendEndpointHost) {
		this.sendEndpointHost = sendEndpointHost;
	}

	public int getSendEndpointPort() {
		return sendEndpointPort;
	}

	public void setSendEndpointPort(int sendEndpointPort) {
		this.sendEndpointPort = sendEndpointPort;
	}

	public String getSendEndpointToken() {
		return sendEndpointToken;
	}

	public void setSendEndpointToken(String sendEndpointToken) {
		this.sendEndpointToken = sendEndpointToken;
		this.streamManager.registerBySendEndpointToken(sendEndpointToken, this);
	}

	public String getReceiveEndpointToken() {
		return receiveEndpointToken;
	}

	public void setReceiveEndpointToken(String receiveEndpointToken) {
		this.receiveEndpointToken = receiveEndpointToken;
	}

	public String getReceiveEndpointHost() {
		return receiveEndpointHost;
	}

	public void setReceiveEndpointHost(String receiveEndpointHost) {
		this.receiveEndpointHost = receiveEndpointHost;
	}

	public int getReceiveEndpointPort() {
		return receiveEndpointPort;
	}

	public void setReceiveEndpointPort(int receiveEndpointPort) {
		this.receiveEndpointPort = receiveEndpointPort;
	}

	public long getGroupId() {
		return groupId;
	}

	public long getStreamId() {
		return streamId;
	}

	public boolean hasExpired(long now) {
		return selectionKey == null && (now - creationTime) > STREAM_EXPIRY_THRESHOLD;
	}

	public void release() {
		streamManager.unregisterStream(this);
	}

	public boolean isActive() {
		return selectionKey != null;
	}
}