package de.tuberlin.cit.livescale.job.util.receiver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.util.encoder.PortBinder;
import de.tuberlin.cit.livescale.job.util.encoder.ServerPortManager;
import de.tuberlin.cit.livescale.job.util.receiver.udpmulticast.UdpClient;
import de.tuberlin.cit.livescale.job.util.source.HostUtil;

public class UdpForwardingReceiver extends Thread implements VideoReceiver {

	private static Log LOG = LogFactory.getLog(UdpForwardingReceiver.class);

	private static final ServerPortManager PORT_MANAGER = new ServerPortManager(43000, 43010);

	private LinkedBlockingQueue<Packet> packetsToSend;

	private String receiveEndpointToken;

	private HashMap<InetSocketAddress, UdpClient> clients;

	private ByteBuffer readBuffer;

	private LinkedList<UdpClient> pendingClients;

	private long groupId;

	private DatagramChannel channel;

	private int serverPort;

	private String receiveEndpointUrl;

	public UdpForwardingReceiver(long groupId, String receiveEndpointToken) throws Exception {
		this.receiveEndpointToken = receiveEndpointToken;
		this.groupId = groupId;

		createDatagramChannel();
		createReceiveEndpointUrl();

		this.packetsToSend = new LinkedBlockingQueue<Packet>();
		this.clients = new HashMap<InetSocketAddress, UdpClient>();
		this.readBuffer = ByteBuffer.allocate(500);
		this.pendingClients = new LinkedList<UdpClient>();
		this.start();
	}

	private void createReceiveEndpointUrl() throws Exception {
		this.receiveEndpointUrl = String.format("udp://%s:d", HostUtil.determineHostAddress(), serverPort);
	}

	@Override
	public void writePacket(Packet packet) throws IOException {
		packetsToSend.add(packet);
	}

	@Override
	public void closeSafely() {
		packetsToSend.clear();
		interrupt();
	}

	@Override
	public long getGroupId() {
		return groupId;
	}

	public void run() {

		try {
			Selector selector = Selector.open();
			SelectionKey clientKey = channel.register(selector, SelectionKey.OP_READ);

			while (!interrupted()) {
				if (!packetsToSend.isEmpty()) {
					drainPacketQueueToReceivers();
				}

				if (!pendingClients.isEmpty()) {
					clientKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
				} else {
					clientKey.interestOps(SelectionKey.OP_READ);
				}

				selector.select(10);

				Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid()) {
						continue;
					}

					if (key.isReadable()) {
						handleAuthOrStop(channel);
					}
					if (key.isWritable()) {
						sendPendingPackets(channel);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			closeChannel();
			PORT_MANAGER.releaseAllocatedPort(serverPort);
			packetsToSend.clear();
			clients.clear();
			pendingClients.clear();
		}
	}

	private void closeChannel() {
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException e) {
			}
			channel = null;
		}
	}

	private void sendPendingPackets(DatagramChannel channel) throws IOException {
		boolean canSendMore = true;

		while (canSendMore) {
			UdpClient client = pendingClients.getFirst();
			boolean complete = client.sendNextPacket(channel);

			if (complete) {
				pendingClients.removeFirst();
				canSendMore = !pendingClients.isEmpty();
			} else {
				canSendMore = false;
			}
		}
	}

	private void drainPacketQueueToReceivers() {
		Packet packet;

		while ((packet = packetsToSend.poll()) != null) {
			for (UdpClient receiver : clients.values()) {
				receiver.enqueuePacket(packet);
				pendingClients.add(receiver);
			}
		}
	}

	private void handleAuthOrStop(DatagramChannel channel) throws IOException {
		InetSocketAddress clientAddress = (InetSocketAddress) channel.receive(readBuffer);

		String message = new String(readBuffer.array(), 0, readBuffer.position(), "US-ASCII");
		readBuffer.clear();

		if (message.equals(String.format("AUTH %s\n", receiveEndpointToken))) {
			if (!clients.containsKey(clientAddress)) {
				LOG.info(String.format("New video-receiver client %s stream-group %d", clientAddress.toString(),
					groupId));
				UdpClient client = new UdpClient(clientAddress);
				clients.put(clientAddress, client);
			}
		} else if (message.equals("STOP\n")) {
			this.clients.remove(clientAddress);
			LOG.info("Removed video-receiver client " + clientAddress.toString());
		}
	}

	private void createDatagramChannel() throws Exception {
		channel = DatagramChannel.open();

		serverPort = PORT_MANAGER.bindToFreePort(new PortBinder() {
			@Override
			public void attemptToBind(int port) throws Exception {
				channel.socket().bind(new InetSocketAddress("0.0.0.0", port));
				LOG.info(String.format("Listening for video-receiver clients on 0.0.0.0:%d for stream %d", port,
					groupId));
			}
		});
		channel.configureBlocking(false);
	}

	@Override
	public String getReceiveEndpointURL() {
		return receiveEndpointUrl;
	}
}
