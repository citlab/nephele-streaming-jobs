package de.tuberlin.cit.livescale.job.util.encoder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.tuberlin.cit.livescale.job.util.decoder.IOBuffer;

public class TCPReceiver extends Thread {

	private static Log LOG = LogFactory.getLog(TCPReceiver.class);

	private static final int PORT_RANGE_BEGIN = 33000;

	private static int PORT_RANGE_END = 33100;

	private static final ServerPortManager PORT_MANAGER = new ServerPortManager(PORT_RANGE_BEGIN, PORT_RANGE_END);

	private IOBuffer ringBuffer = new IOBuffer("EncoderBuffer");

	private int allocatedPort;

	private boolean ready = false;

	public void run() {
		ServerSocket serverSocket = null;
		Socket socket = null;

		try {
			serverSocket = createServerSocket();

			synchronized (ringBuffer) {
				ready = true;
				ringBuffer.notifyAll();
			}

			socket = serverSocket.accept();
			InputStream in = socket.getInputStream();

			byte[] tmpBuffer = new byte[1024];

			while (!interrupted()) {
				int read = in.read(tmpBuffer);
				if (read == -1) {
					break;
				} else {
					int written = 0;
					while (written < read) {
						synchronized (ringBuffer) {
							written += ringBuffer.write(tmpBuffer, 0, read);
						}
						if (written < read) {
							sleep(10);
						}
					}
				}
			}
		} catch (InterruptedException e) {
			// do nothing
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					// do nothing
				}
			}
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					// do nothing
				}
			}
			PORT_MANAGER.releaseAllocatedPort(allocatedPort);
		}
	}

	private ServerSocket createServerSocket() throws Exception {
		final ServerSocket sock = new ServerSocket();
		allocatedPort = PORT_MANAGER.bindToFreePort(new PortBinder() {
			@Override
			public void attemptToBind(int port) throws Exception {
				sock.bind(new InetSocketAddress("127.0.0.1", port));
			}
		});
		return sock;
	}

	public String getUrl() {
		return String.format("tcp://127.0.0.1:%d", allocatedPort);
	}

	public boolean hasDataAvailableForReading() {
		synchronized (ringBuffer) {
			return ringBuffer.availableForReading() > 0;
		}
	}

	public byte[] readAvailableData() {
		synchronized (ringBuffer) {
			int available = ringBuffer.availableForReading();
			byte[] data = new byte[available];
			ringBuffer.read(data, available);
			return data;
		}
	}

	public void startAndWaitUntilReady() throws InterruptedException {
		this.start();
		synchronized (ringBuffer) {
			if (!ready) {
				ringBuffer.wait();
			}
		}
	}
}
