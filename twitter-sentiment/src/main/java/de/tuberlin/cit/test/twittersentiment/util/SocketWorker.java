package de.tuberlin.cit.test.twittersentiment.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

public class SocketWorker implements Runnable {
	private static final Logger LOG = Logger.getLogger(SocketWorker.class);
	private ObjectMapper mapper = new ObjectMapper();
	private BlockingQueue<JsonNode> queue;
	private int port;

	SocketWorker(BlockingQueue<JsonNode> queue, int port) {
		this.queue = queue;
		this.port = port;
	}

	@Override
	public void run() {
		ServerSocket serverSocket = null;
		Socket socket = null;
		BufferedReader in = null;

		try {
			serverSocket = new ServerSocket(port);
			socket = serverSocket.accept();
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			String line = null;
			while ((line = in.readLine()) != null) {
				JsonNode tweet = mapper.readValue(line, JsonNode.class);

				// strip unnecessary information
				ObjectNode filteredTweet = new ObjectMapper().createObjectNode();
				String[] includeProperties = { "id", "text", "lang", "entities", "created_at" };
				for (String property : includeProperties) {
					filteredTweet.set(property, tweet.get(property));
				}

				queue.put(filteredTweet);
			}

		} catch (IOException e) {
			LOG.error("I/O error in socket worker.", e);

		} catch (InterruptedException e) {
			LOG.info("Socket worker interrupted.");

		} finally {
			try {
				in.close();
				socket.close();
				serverSocket.close();
			} catch (IOException e) {
				LOG.error("Failed to close socket worker server port.", e);
				e.printStackTrace();
			}
		}
	}
}
