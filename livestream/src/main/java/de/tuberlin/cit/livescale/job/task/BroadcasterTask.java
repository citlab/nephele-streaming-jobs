package de.tuberlin.cit.livescale.job.task;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.cit.livescale.job.util.receiver.LatencyLog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.util.receiver.FileVideoReceiver;
import de.tuberlin.cit.livescale.job.util.receiver.FlvOverTcpForwardingReceiver;
import de.tuberlin.cit.livescale.job.util.receiver.MpegTsHttpServerReceiver;
import de.tuberlin.cit.livescale.job.util.receiver.UdpForwardingReceiver;
import de.tuberlin.cit.livescale.job.util.receiver.VideoReceiver;
import de.tuberlin.cit.livescale.messaging.MessageCenter;
import de.tuberlin.cit.livescale.messaging.MessageListener;
import de.tuberlin.cit.livescale.messaging.messages.BroadcasterStreamAnnounceRequest;
import de.tuberlin.cit.livescale.messaging.messages.StreamserverStreamAnnounceAnswer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

public class BroadcasterTask extends AbstractOutputTask {

	private static final Log LOG = LogFactory.getLog(BroadcasterTask.class);

	public static final String BROADCAST_TRANSPORT = "BROADCAST_TRANSPORT";

	public static final String DEFAULT_BROADCAST_TRANSPORT = "http";

	public static final String SETUP_AMQP_MESSAGING = "SETUP_AMQP_MESSAGING";

	public static final boolean DEFAULT_SETUP_AMQP_MESSAGING = false;

	private static final String CONFIG_FILE_NAME = "broadcaster-messaging.properties";

	public static final String LATENCY_LOG = "BROADCAST_LATENCY_LOG";

	private static final String LATENCY_LOG_DEFAULT = "/tmp/qos_statistics_receiver";

	private MessageCenter messageCenter;

	private RecordReader<Packet> reader;

	private ConcurrentHashMap<Long, VideoReceiver> groupId2Receiver = new ConcurrentHashMap<Long, VideoReceiver>(
			16, 0.75f, 1);

	@Override
	public void registerInputOutput() {
		this.reader = new RecordReader<Packet>(this, Packet.class);
	}

	@Override
	public void invoke() throws Exception {

		setupMessagingIfNecessary();

		LatencyLog latLogger = null;
		try {
			latLogger = new LatencyLog(getTaskConfiguration().getString(
							LATENCY_LOG, LATENCY_LOG_DEFAULT));

			while (this.reader.hasNext()) {
				Packet packet = this.reader.next();
				if (packet.isDummyPacket()) {
					continue;
				}
				if (packet.getTimestamp() != -1) {
					latLogger.log(packet.getTimestamp());
				}
				VideoReceiver receiver = this.groupId2Receiver.get(packet
						.getGroupId());
				if (receiver != null) {
					if (!packet.isEndOfStreamPacket()) {
						try {
							receiver.writePacket(packet);
						} catch (IOException e) {
							dropReceiver(receiver);
						}
					} else {
						dropReceiver(receiver);
					}
				} else {
					createVideoReceiver(packet.getGroupId(),
							UUID.randomUUID().toString()).writePacket(packet);
				}
			}
		} catch (InterruptedException e) {
		} finally {
			if (latLogger != null) {
				latLogger.close();
			}
		}


		shutdown();
	}

	private void dropReceiver(VideoReceiver receiver) {
		LOG.info("Closing receiver for stream-group " + receiver.getGroupId());
		receiver.closeSafely();
		this.groupId2Receiver.remove(receiver.getGroupId());
	}

	private void shutdown() {
		for (VideoReceiver receiver : this.groupId2Receiver.values()) {
			receiver.closeSafely();
		}
		this.groupId2Receiver.clear();
	}

	private VideoReceiver createVideoReceiver(long groupId,
			String receiveEndpointToken) throws Exception {

		VideoReceiver receiver = null;
		try {

			String broadcastTransport = getTaskConfiguration().getString(
					BROADCAST_TRANSPORT, DEFAULT_BROADCAST_TRANSPORT);

			if (broadcastTransport.startsWith("file://")) {
				receiver = new FileVideoReceiver(groupId, broadcastTransport
						+ groupId);
			} else if (broadcastTransport.startsWith("flv2tcp")) {
				receiver = new FlvOverTcpForwardingReceiver(groupId);
			} else if (broadcastTransport.startsWith("udp")) {
				receiver = new UdpForwardingReceiver(groupId,
						receiveEndpointToken);
			} else if (broadcastTransport.startsWith("http")) {
				receiver = new MpegTsHttpServerReceiver(groupId,
						receiveEndpointToken);
			} else {
				throw new RuntimeException("Unkown broadcast transport: "
						+ broadcastTransport.toString());
			}
			this.groupId2Receiver.put(groupId, receiver);
		} catch (IOException e) {
			LOG.error("Error when creating video receiver for stream-group "
					+ groupId, e);
		}

		return receiver;
	}

	public void handleStreamAnnounceMessage(BroadcasterStreamAnnounceRequest msg)
			throws Exception {

		int targetReceiver = (int) (msg.getGroupId() % getCurrentNumberOfSubtasks());

		LOG.info("Received new stream announce event");
		if (targetReceiver == getIndexInSubtaskGroup()) {
			LOG.info("Handling new stream announce event");
			VideoReceiver receiver = createVideoReceiver(msg.getGroupId(),
					msg.getReceiveEndpointToken());

			StreamserverStreamAnnounceAnswer reply = (StreamserverStreamAnnounceAnswer) msg
					.getResponseMessage();
			reply.setStreamId(msg.getStreamId());
			reply.setGroupId(msg.getGroupId());
			reply.setSendEndpointToken(msg.getSendEndpointToken());
			reply.setReceiveEndpointUrl(receiver.getReceiveEndpointURL());
			this.messageCenter.sendResponse(reply);
		}
	}

	private void setupMessagingIfNecessary() throws IOException {
		if (!this.getTaskConfiguration().getBoolean(SETUP_AMQP_MESSAGING,
				DEFAULT_SETUP_AMQP_MESSAGING)) {
			return;
		}

		try {
			URL url = this.getClass().getClassLoader()
					.getResource(CONFIG_FILE_NAME);
			LOG.info(String.format("Loading messaging config from %s", url));
			InputStream is = this.getClass().getClassLoader()
					.getResourceAsStream(CONFIG_FILE_NAME);

			Properties properties = new Properties();
			properties.load(is);

			this.messageCenter = new MessageCenter(false, properties);
			this.messageCenter.addMessageListener(
					BroadcasterStreamAnnounceRequest.class,
					new MessageListener<BroadcasterStreamAnnounceRequest>() {
						@Override
						public void handleMessageReceived(
								BroadcasterStreamAnnounceRequest message) {
							LOG.info(String.format("Received message: %s",
									message.getClass().getSimpleName()));
							try {
								handleStreamAnnounceMessage(message);
							} catch (Exception e) {
								LOG.error(e);
							}
						}
					});
			this.messageCenter.startAllEndpoints();

		} catch (IOException e) {
			LOG.error("Could not start Message center", e);
			throw e;
		}
	}

}
