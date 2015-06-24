package de.tuberlin.cit.livescale.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import de.tuberlin.cit.livescale.job.util.source.Livestream;

public class LivestreamClient {

	private int noOfStreams;

	private File videoFile;

	private boolean videoFileIsPacketized;
	
	private boolean producePacketizedStream;

	private String server;

	private int port;

	private byte[] videoFileContents;

	/**
	 * Number of currently open connections.
	 */
	private int connected;

	/**
	 * Java NIO selector
	 */
	private Selector selector;

	private Map<SelectionKey, Livestream> key2Stream = new HashMap<SelectionKey, Livestream>();

	/**
	 * Holds a buffer for each SelectionKey registered with the Selector (one SelectionKey/buffer pair per open
	 * connection)
	 */
	private Map<SelectionKey, ByteBuffer> writeBuffers = new HashMap<SelectionKey, ByteBuffer>();

	/**
	 * Mapping between SelectionKey and its corresponding PrioritizedSelectionKe object
	 */
	private Map<SelectionKey, PrioritizedSelectionKey> key2Prio = new HashMap<SelectionKey, PrioritizedSelectionKey>();

	/**
	 * Priority queue of selection keys (specifies when to send data on a connection)
	 */
	private PriorityQueue<PrioritizedSelectionKey> pendingKeys = new PriorityQueue<PrioritizedSelectionKey>();

	public LivestreamClient(int noOfStreams, File packetizedVideoFile, boolean videoFileIsPacketized, boolean producePacketizedStream, String server,
			int port) {
		this.noOfStreams = noOfStreams;
		this.videoFile = packetizedVideoFile;
		this.videoFileIsPacketized = videoFileIsPacketized;
		this.producePacketizedStream = producePacketizedStream;
		this.server = server;
		this.port = port;
	}

	private void preloadVideoFile() throws IOException {
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(videoFile));
		videoFileContents = new byte[in.available()];
		in.read(videoFileContents);
		in.close();
	}

	public void openConnections() throws IOException {
		selector = SelectorProvider.provider().openSelector();

		for (int i = 0; i < noOfStreams; i++) {
			openNewConnection();
		}

		while (connected < noOfStreams) {
			selector.select();

			Iterator<SelectionKey> i = selector.selectedKeys().iterator();

			while (i.hasNext()) {
				SelectionKey sk = (SelectionKey) i.next();
				i.remove();

				if (!sk.isValid()) {
					closeConnectionAndCleanup(sk);
					openNewConnection();
				} else if (sk.isConnectable()) {
					finishConnection(sk);
				}
			}
		}

		System.out.println(String.format("Established %s connections", connected));
		System.out.println("checking whether all connections are open");
		for (SelectionKey key : writeBuffers.keySet()) {
			SocketChannel channel = (SocketChannel) key.channel();
			if (!channel.isOpen()) {
				System.out.println("Channel is not open");
			}

			if (!channel.isConnected()) {
				System.out.println("Channel is not connected");
			}
			if (channel.isConnectionPending()) {
				System.out.println("Channel has connection pending");
			}
		}
	}

	private void finishConnection(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		try {
			if (socketChannel.finishConnect()) {
				key.interestOps(0);

				long now = System.currentTimeMillis();
//				long streamStartTime = (long) (now + 5000 + (10000 * Math.random()));
				long streamStartTime = now;
				Livestream livestream = createLivestream(streamStartTime);
				key2Stream.put(key, livestream);

				ByteBuffer writeBuffer = ByteBuffer.allocate(500 * 1024);
				livestream.fillBufferWithCurrentPacket(writeBuffer);
				writeBuffers.put(key, writeBuffer);

				System.out.printf("New stream will start streaming in %d millis\n", streamStartTime - now);
				socketChannel.socket().setTcpNoDelay(true);
				createPrioritizedSelectionKey(key);

				connected++;
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			// one connection attempt failed, make a new one
			closeConnectionAndCleanup(key);
			openNewConnection();
		}
	}

	private Livestream createLivestream(long streamStartTime) {
		return new Livestream(videoFileContents, videoFileIsPacketized, streamStartTime, producePacketizedStream);
	}

	private void openNewConnection() {
		try {
			SocketChannel socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketChannel.register(selector, SelectionKey.OP_CONNECT);
			socketChannel.connect(new InetSocketAddress(server, port));
		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
		}
	}

	private void closeConnectionAndCleanup(SelectionKey key) {
		key.cancel();

		SocketChannel channel = (SocketChannel) key.channel();
		try {
			channel.close();
		} catch (IOException e) {
		}
		connected--;

		writeBuffers.remove(key);
		key2Stream.remove(key);
		key2Prio.remove(key);

		System.out.println("Connections-1: " + connected);
	}

	private void streamData() {
		System.out.println("Starting to stream data");
		try {
			while (connected > 0) {

				requestWriteOnExpiredSelectionKeys();

				// selector.select() returns either when a connection is ready for writing
				// or when the next connection needs to be set to writable (->timeout)
				long timeOut = 1;
				if (!pendingKeys.isEmpty()) {
					timeOut = Math.max(1, pendingKeys.peek().timeOfNextWrite - System.currentTimeMillis());
				}
				selector.select(timeOut);

				Iterator<SelectionKey> i = selector.selectedKeys().iterator();

				while (i.hasNext()) {

					SelectionKey sk = (SelectionKey) i.next();
					i.remove();

					if (!sk.isValid()) {
						closeConnectionAndCleanup(sk);
						openNewConnection();
					} else if (sk.isWritable()) {
						ByteBuffer buffer = writeBuffers.get(sk);
						boolean success = write(sk, buffer);

						if (success && !buffer.hasRemaining()) {
							long timediff = System.currentTimeMillis() - key2Stream.get(sk).getTimeOfWriteForCurrentPacket(); 
							if(timediff > 12 || timediff < 0) {
								System.out.println("timediff: " + timediff);
							}
							refillBufferOrRestartStream(sk, buffer);
						}
					} else if (sk.isConnectable()) {
						finishConnection(sk);
					}
				}
			}

			System.out.println("Terminating because all connections died");
		} catch (IOException io) {
			System.out.println(io.getMessage());
			io.printStackTrace(System.out);
		}

		shutdown();
	}

	private void shutdown() {
		for (SelectionKey key : key2Stream.keySet()) {
			closeConnectionAndCleanup(key);
		}
		pendingKeys.clear();
	}

	private void refillBufferOrRestartStream(SelectionKey key, ByteBuffer buffer) {
		key.interestOps(0);
		Livestream stream = key2Stream.get(key);
		stream.shiftToNextPacket();

		if (stream.isEOF()) {
			System.out.println("Closing connection due to EOF on underlying file.");
			closeConnectionAndCleanup(key);
			openNewConnection();
		} else {
			stream.fillBufferWithCurrentPacket(buffer);
			PrioritizedSelectionKey pendingKey = key2Prio.get(key);
			pendingKey.timeOfNextWrite = stream.getTimeOfWriteForCurrentPacket();
			pendingKeys.add(pendingKey);
		}
	}

	private void requestWriteOnExpiredSelectionKeys() {
		long now = System.currentTimeMillis();
		while (!pendingKeys.isEmpty() && pendingKeys.peek().timeOfNextWrite <= now) {
			SelectionKey pendingKey = pendingKeys.remove().key;
			pendingKey.interestOps(SelectionKey.OP_WRITE);
		}
	}

	private void createPrioritizedSelectionKey(SelectionKey key) {
		Livestream livestream = key2Stream.get(key);

		PrioritizedSelectionKey pendingKey = new PrioritizedSelectionKey();
		pendingKey.timeOfNextWrite = livestream.getTimeOfWriteForCurrentPacket();
		pendingKey.key = key;

		key2Prio.put(key, pendingKey);
		pendingKeys.add(pendingKey);
	}

	private boolean write(SelectionKey key, ByteBuffer buffer) {
		SocketChannel channel = (SocketChannel) key.channel();

		boolean success = false;
		try {
			channel.write(buffer);
			success = true;
		} catch (IOException e) {
			closeConnectionAndCleanup(key);
			openNewConnection();
		}
		return success;
	}

	class PrioritizedSelectionKey implements Comparable<PrioritizedSelectionKey> {
		long timeOfNextWrite;

		SelectionKey key;

		@Override
		public int compareTo(PrioritizedSelectionKey other) {
			if (timeOfNextWrite < other.timeOfNextWrite) {
				return -1;
			} else if (timeOfNextWrite > other.timeOfNextWrite) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private void doStream() throws IOException {
		preloadVideoFile();
		openConnections();
		streamData();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Usage: livestreamclient <config-file>");
			System.exit(1);
		}

		File configFile = new File(args[0]);
		if (!configFile.exists() || !configFile.canRead()) {
			System.out.printf("File not found or not readable: %s\n", args[0]);
			System.exit(1);
		}

		try {
			LivestreamClient client = parseConfiguration(configFile);
			client.doStream();

		} catch (IOException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			System.exit(1);
		}
	}

	private static LivestreamClient parseConfiguration(File configFile) throws FileNotFoundException, IOException {
		Properties props = new Properties();
		FileInputStream fin = new FileInputStream(configFile);
		props.load(fin);
		fin.close();

		int noOfStreams = Integer.parseInt(props.getProperty("noOfStreams"));

		File videoFile;
		boolean videoFileIsPacketized;
		if (props.containsKey("packetizedVideoFile")) {
			videoFile = new File(props.getProperty("packetizedVideoFile"));
			videoFileIsPacketized = true;
		} else {
			videoFile = new File(props.getProperty("videoFile"));
			videoFileIsPacketized = false;
		}
		
		boolean producePacketizedStream = false;
		if (props.containsKey("producePacketizedStream")) {
			producePacketizedStream = props.getProperty("producePacketizedStream").equals("true");
		}

		String server = props.getProperty("server");
		int port = Integer.parseInt(props.getProperty("port"));

		if (!videoFile.exists() || !videoFile.canRead()) {
			System.out.printf("File not found or not readable: %s\n", props.getProperty("packetizedVideoFile"));
			System.exit(1);
		}
		
		System.out.println("================= CONFIGURATION ================= ");
		System.out.println(String.format("Configuration file: ", configFile.getAbsolutePath()));
		System.out.println(String.format("Number of streams: %d", noOfStreams));
		System.out.println(String.format("Video file: %s (is packetized: %b)", videoFile.getAbsoluteFile(), videoFileIsPacketized));
		System.out.println(String.format("Produce packetized video stream: %b", producePacketizedStream));
		System.out.println(String.format("Stream to: %s:%d", server, port));
		System.out.println("================================================= ");
		
		LivestreamClient client = new LivestreamClient(noOfStreams, videoFile, videoFileIsPacketized, producePacketizedStream, server, port);
		return client;
	}
}
