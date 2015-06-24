package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.livescale.job.util.encoder.PortBinder;
import de.tuberlin.cit.livescale.job.util.encoder.ServerPortManager;
import de.tuberlin.cit.livescale.job.util.source.HostUtil;

public class FlvStreamForwarderThread extends Thread {

	private static final Log LOG = LogFactory.getLog(FlvStreamForwarderThread.class);

	/**
	 * Each frame has a timestamp that determines when it is to be displayed. In order to be displayed the frame
	 * has to be forwarded via the client connection, decoded etc. This offset determines how many many milliseconds
	 * before its timestamp the frame is to be forwarded to the client connection.
	 */
	private final long FORWARDING_OFFSET_IN_MILLIS = 3000;

	private BufferedFlvFile bufferedFile;

	private ServerSocket serverSocket;

	private Socket clientSocket;

	private OutputStream clientSocketOutput;

	private int serverPort;

	private long groupId;

	private ServerPortManager portManager;

	private String receiveEndPointUrl;

	public FlvStreamForwarderThread(long groupId, BufferedFlvFile bufferedFile, ServerPortManager portManager)
			throws Exception {

		this.bufferedFile = bufferedFile;
		this.groupId = groupId;
		this.portManager = portManager;
		this.serverSocket = createServerSocket();
		this.receiveEndPointUrl = String.format("tcp://%s:%d", HostUtil.determineHostAddress(), serverPort);
	}
	
	public long getGroupId() {
		return groupId;
	}

	public void run() {
		try {

			while (!interrupted()) {
				LOG.info("Waiting until FLV buffer full");
				bufferedFile.waitUntilBufferFull();
				try {
					handleNextClient();
				} catch (IOException e) {
					// do nothing
				} finally {
					closeClientConnectionIfNecessary();
					bufferedFile.setDropExcessFrames(true);
				}
				sleep(100);
			}

		} catch (Exception e) {
			if (!(e instanceof InterruptedException)) {
				LOG.error(e.getMessage(), e);
			}
		} finally {
			closeClientConnectionIfNecessary();
			closeServerSocket();
			portManager.releaseAllocatedPort(serverPort);
		}
	}

	private void closeClientConnectionIfNecessary() {
		if (clientSocket != null) {
			LOG.info("Closing client connection");
			try {
				clientSocket.close();
			} catch (IOException e) {
			}
			clientSocket = null;
			clientSocketOutput = null;
		}
	}

	private void closeServerSocket() {
		try {
			serverSocket.close();
		} catch (IOException e) {
		}
		serverSocket = null;
	}

	private ServerSocket createServerSocket() throws Exception {
		final ServerSocket socket = new ServerSocket();

		this.serverPort = portManager.bindToFreePort(new PortBinder() {
			@Override
			public void attemptToBind(int port) throws Exception {
				socket.bind(new InetSocketAddress("0.0.0.0", port));
				LOG.info(String.format("Listening for video-receiver clients on 0.0.0.0:%d for stream-group %d", port,
					groupId));
			}
		});

		return socket;
	}

	private void handleNextClient() throws IOException, InterruptedException {
		clientSocket = this.serverSocket.accept();
		LOG.info("Client connected.");
		clientSocket.setTcpNoDelay(true);
		clientSocketOutput = clientSocket.getOutputStream();

		forwardFlvFileAtom(bufferedFile.getFlvFileHeader());
		forwardFlvFileAtom(bufferedFile.getFlvMetaTag());
		forwardFlvFileAtom(bufferedFile.getFlvAvcConfigVideoTag());

		bufferedFile.setDropExcessFrames(false);
		FlvTag firstKeyframe = bufferedFile.dequeueFlvTag();

		long startTimeInNanos = System.currentTimeMillis();
		long timebase = firstKeyframe.getTimestamp();
		firstKeyframe.rebaseTimestamp(timebase);
		forwardFlvFileAtom(firstKeyframe);

		while (!interrupted()) {
			FlvTag nextFrame = bufferedFile.dequeueFlvTag();
			nextFrame.rebaseTimestamp(timebase);

			long timePassedSinceStart = System.currentTimeMillis() - startTimeInNanos;
			long sleepTime = nextFrame.getTimestamp() - FORWARDING_OFFSET_IN_MILLIS - timePassedSinceStart;
			if (sleepTime > 0) {
				sleep(sleepTime);
			}
			forwardFlvFileAtom(nextFrame);
		}
	}

	private void forwardFlvFileAtom(FlvFileAtom fileAtom) throws IOException {
		ByteBuffer atomBuffer = fileAtom.getBufferForReading();
		clientSocketOutput.write(atomBuffer.array(), 0, atomBuffer.limit());
	}

	public String getReceiveEndpointURL() {
		return receiveEndPointUrl;
	}
}
