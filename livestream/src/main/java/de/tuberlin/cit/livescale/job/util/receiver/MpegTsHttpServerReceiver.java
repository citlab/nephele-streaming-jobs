package de.tuberlin.cit.livescale.job.util.receiver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.util.encoder.PortBinder;
import de.tuberlin.cit.livescale.job.util.encoder.ServerPortManager;
import de.tuberlin.cit.livescale.job.util.receiver.tcpserver.TcpClient;
import de.tuberlin.cit.livescale.job.util.source.HostUtil;

public class MpegTsHttpServerReceiver extends Thread implements VideoReceiver {

	private static Log LOG = LogFactory.getLog(MpegTsHttpServerReceiver.class);

	private LinkedBlockingQueue<Packet> packetsToSend;

	private String receiveEndpointToken;

	private static final ServerPortManager PORT_MANAGER = new ServerPortManager(43001, 43100);

	private int connectedClients = 0;

	private ServerSocketChannel serverChannel;

	private HashMap<SelectionKey, TcpClient> clients;

	private ByteBuffer readBuffer;

	private long groupId;

	private String receiveEndpointUrl;

	private int serverPort;

	public MpegTsHttpServerReceiver(long groupId, String receiveEndpointToken) throws Exception {
		this.groupId = groupId;
		this.receiveEndpointToken = receiveEndpointToken;
		createServerSocket();
		createReceiveEndpointUrl();
		this.packetsToSend = new LinkedBlockingQueue<Packet>();
		this.clients = new HashMap<SelectionKey, TcpClient>();
		this.readBuffer = ByteBuffer.allocate(1024);
		this.start();
	}

	private void createReceiveEndpointUrl() throws SocketException {
		this.receiveEndpointUrl = String.format("http://%s:%d/live.ts?auth=%s", HostUtil.determineHostAddress(),
			this.serverPort, this.receiveEndpointToken);
	}

	public void run() {

		try {
			Selector selector = SelectorProvider.provider().openSelector();
			serverChannel.register(selector, SelectionKey.OP_ACCEPT);

			LOG.info(String.format("Listening for video-receiver clients on %s for stream %d", receiveEndpointUrl,
				groupId));

			while (!Thread.interrupted()) {
				if (!packetsToSend.isEmpty()) {
					drainPacketQueueToClients();
				}

				if (selector.select(10) == 0) {
					if (!Thread.interrupted()) {
						continue;
					} else {
						break;
					}
				}

				Iterator<SelectionKey> i = selector.selectedKeys().iterator();

				while (i.hasNext()) {

					SelectionKey key = (SelectionKey) i.next();
					i.remove();

					if (!key.isValid()) {
						closeConnectionAndCleanup(key);
					} else if (key.isWritable()) {
						writePacketData(key);
					} else if (key.isAcceptable()) {
						acceptConnection(key, selector);
					} else if (key.isReadable()) {
						readCommand(key);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}

		shutdown();
	}

	private void writePacketData(SelectionKey key) throws IOException, InterruptedException {
		TcpClient client = clients.get(key);
		boolean noDataLeft;
		try {
			noDataLeft = client.sendPacketData((SocketChannel) key.channel());
			if (noDataLeft) {
				key.interestOps(SelectionKey.OP_READ);
			}
		} catch (IOException e) {
			closeConnectionAndCleanup(key);
		}
	}

	private void drainPacketQueueToClients() {
		Packet packet;

		while ((packet = packetsToSend.poll()) != null) {
			for (SelectionKey key : clients.keySet()) {
				TcpClient client = clients.get(key);

				if (client.isReceiving()) {
					client.enqueuePacket(packet);
					key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
				}
			}
		}
	}

	private void readCommand(SelectionKey key) throws InterruptedException, IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		try {
			readBuffer.clear();
			int read = socketChannel.read(readBuffer);
			readBuffer.flip();

			// -1 means we have reached end of stream
			if (read == -1) {
				closeConnectionAndCleanup(key);
			} else {
				// handleCommand(key, new String(readBuffer.array(), 0, readBuffer.limit(), "US-ASCII"));
				handleHTTPRequest(key, readBuffer);
			}
		} catch (IOException e) {
			closeConnectionAndCleanup(key);
		}
	}

	private void handleHTTPRequest(SelectionKey key, ByteBuffer buffer) throws IOException, InterruptedException {
		String request = new String(buffer.array(), 0, buffer.limit(), "US-ASCII");
		LOG.info(request);

		String[] requestLines = request.split("\r\n");

		if (requestLines[0].equals(String.format("GET /live.ts?auth=%s HTTP/1.1", receiveEndpointToken))) {
			StringBuilder sb = new StringBuilder();
			sb.append("HTTP/1.1 200 OK\r\n");
			sb.append("Content-Type: video/MP2T\r\n");
			sb.append("Connection: close\r\n");
			sb.append("Accept-Ranges: bytes\r\n");
			sb.append("Content-Disposition: inline; filename=live.ts\r\n\r\n");

			Packet responsePacket = new Packet(0, 0, 0, sb.toString().getBytes("US-ASCII"), 0);
			clients.get(key).enqueuePacket(responsePacket);
			clients.get(key).setReceiving(true);
		} else {
			closeConnectionAndCleanup(key);
		}
	}

	private void acceptConnection(SelectionKey serverChannelKey, Selector selector) throws IOException {
		ServerSocketChannel channel = (ServerSocketChannel) serverChannelKey.channel();
		SocketChannel clientChannel = channel.accept();
		clientChannel.socket().setTcpNoDelay(true);
		clientChannel.configureBlocking(false);
		SelectionKey clientChannelKey = clientChannel.register(selector, SelectionKey.OP_WRITE);

		TcpClient client = new TcpClient();
		clients.put(clientChannelKey, client);

		connectedClients++;
		LOG.info("Connected video-receiver clients+1: " + connectedClients);
	}

	private void closeConnectionAndCleanup(SelectionKey key) throws IOException, InterruptedException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		key.cancel();
		try {
			socketChannel.close();
		} catch (IOException e1) {
		}

		clients.remove(key);
		connectedClients--;
		LOG.info("Connected video-receiver clients-1: " + connectedClients);
	}

	private void createServerSocket() throws Exception {
		serverChannel = ServerSocketChannel.open();

		this.serverPort = PORT_MANAGER.bindToFreePort(new PortBinder() {
			@Override
			public void attemptToBind(int port) throws Exception {
				serverChannel.socket().bind(new InetSocketAddress("0.0.0.0", port));
			}
		});

		serverChannel.configureBlocking(false);
	}

	private void shutdown() {
		// close all channels
		try {
			serverChannel.close();
		} catch (IOException e) {
		}

		for (SelectionKey key : clients.keySet()) {
			try {
				key.channel().close();
			} catch (IOException e) {
			}
			key.cancel();
		}

		// clear large memory structures
		clients.clear();
		PORT_MANAGER.releaseAllocatedPort(serverPort);
		LOG.info(String.format("Shutting down video-receiver on %s for stream %d", receiveEndpointUrl,
				groupId));
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

	@Override
	public String getReceiveEndpointURL() {
		return receiveEndpointUrl;
	}
}
