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

public class SocketWorker implements Runnable {
	private ObjectMapper mapper = new ObjectMapper();
	private BlockingQueue<JsonNode> queue;
	private int port;

	SocketWorker(BlockingQueue<JsonNode> queue, int port) {
		this.queue = queue;
		this.port = port;
	}

	@Override
	public void run() {
		ServerSocket serverSocket;
		Socket socket;
		BufferedReader in;

		try {
			serverSocket = new ServerSocket(port);
			socket = serverSocket.accept();
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		String line = null;
		do {
			try {
				line = in.readLine();

				JsonNode tweet = mapper.readValue(line, JsonNode.class);

				// strip unnecessary information
				ObjectNode filteredTweet = new ObjectMapper().createObjectNode();
				String[] includeProperties = {"id",  "text", "lang", "entities"};
				for (String property : includeProperties) {
					filteredTweet.set(property, tweet.get(property));
				}

				queue.put(filteredTweet);

			} catch (InterruptedException | IOException ignored) {
			}
		} while (line != null);

		try {
			in.close();
			socket.close();
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
