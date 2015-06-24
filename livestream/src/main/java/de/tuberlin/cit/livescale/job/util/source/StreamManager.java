package de.tuberlin.cit.livescale.job.util.source;

import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.tuberlin.cit.livescale.messaging.messages.StreamserverNewStream;

public class StreamManager {

	private Map<SelectionKey, Stream> key2Stream = new HashMap<SelectionKey, Stream>();

	private Map<String, Stream> sendToken2Stream = new HashMap<String, Stream>();

	private IdGenerator idGenerator;

	private String hostAddress;

	private int serverPort;

	private int gcCounter;

	public StreamManager(IdGenerator idGenerator, int serverPort)
			throws Exception {
		this.idGenerator = idGenerator;
		this.serverPort = serverPort;
		this.hostAddress = determineHostAddress();
		// FIXME garbage collect sendToken2Streammap in periodic intervals
	}

	private String determineHostAddress() throws SocketException, Exception {
		String address = HostUtil.determineHostAddress();
		if (address == null) {
			throw new Exception(
					"Unable to determine IP address of underlying host.");
		}

		return address;
	}

	public Stream createInitialStream(StreamserverNewStream msg)
			throws NoSuchAlgorithmException {
		garbageCollectStreamsIfNecessary();

		long streamId = byteArrayToLong(MessageDigest.getInstance("SHA")
				.digest(msg.getSendEndpointToken().getBytes()));
		long groupId = streamId;
		Stream stream = new Stream(this, streamId, groupId);
		stream.setSendEndpointToken(msg.getSendEndpointToken());
		stream.setReceiveEndpointToken(msg.getReceiveEndpointToken());
		stream.setSendEndpointHost(this.hostAddress);
		stream.setSendEndpointPort(this.serverPort);
		return stream;
	}

	private long byteArrayToLong(final byte[] ba) {
		long l = 0;

		for (int i = 0; i < 8; ++i) {
			l |= (ba[8 - 1 - i] & 0xffL) << (i << 3);
		}

		return l;
	}

	private void garbageCollectStreamsIfNecessary() {
		gcCounter++;
		if (gcCounter >= 5) {
			garbageCollectStreams();
			gcCounter = 0;
		}
	}

	/**
	 * Removes expired streams from sendToken2Stream.
	 */
	private void garbageCollectStreams() {
		long now = System.currentTimeMillis();
		Iterator<Entry<String, Stream>> iterator = sendToken2Stream.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<String, Stream> entry = iterator.next();
			if (entry.getValue().hasExpired(now)) {
				System.out.println("Garbage collected 1 stream");
				iterator.remove();
			}
		}
	}

	public Stream getStream(SelectionKey key) {
		return key2Stream.get(key);
	}

	public Stream getStream(String sendEndpointToken) {
		return sendToken2Stream.get(sendEndpointToken);
	}

	public void unregisterStream(Stream stream) {
		if (stream.getSelectionKey() != null) {
			key2Stream.remove(stream.getSelectionKey());
		}

		if (stream.getSendEndpointToken() != null) {
			sendToken2Stream.remove(stream.getSendEndpointToken());
		}
	}

	public List<Stream> getAllActiveStreams() {
		return new LinkedList<Stream>(key2Stream.values());
	}

	public void registerBySelectionKey(SelectionKey selectionKey, Stream stream) {
		key2Stream.put(selectionKey, stream);
	}

	public void registerBySendEndpointToken(String sendEndpointToken,
			Stream stream) {
		sendToken2Stream.put(sendEndpointToken, stream);
	}
}
