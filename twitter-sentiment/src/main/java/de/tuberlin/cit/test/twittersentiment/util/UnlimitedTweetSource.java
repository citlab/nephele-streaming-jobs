package de.tuberlin.cit.test.twittersentiment.util;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;

public class UnlimitedTweetSource extends TweetSource {

	private static final int TWEET_QUEUE_SIZE = 10000;
	private static final Logger LOG = Logger.getLogger(UnlimitedTweetSource.class);
	private static final long LOG_STATS = 10 * 1000; // log stats every 10s

	/** Wait ...ms for input or finish (return null as tweet). */
	public static final String INPUT_TIMEOUT_KEY = "unlimitedTweetSource.inputTimeout";
	public static final long DEFAULT_INPUT_TIMEOUT = 2*60*1000; // 2min
	private final long inputTimeout;

	private int tweetsEmitted;

	private long lastStatisticLogged;

	private BlockingQueue<JsonNode> queue;

	private Thread socketWorkerThread;

	private int port;

	public UnlimitedTweetSource(Configuration taskConfiguration, int port) {
		this.port = port;
		this.tweetsEmitted = 0;
		this.lastStatisticLogged = System.currentTimeMillis();
		this.inputTimeout = taskConfiguration.getLong(INPUT_TIMEOUT_KEY, DEFAULT_INPUT_TIMEOUT);
	}

	public JsonNode getTweet() throws InterruptedException, IOException {

		long now = System.currentTimeMillis();

		if (queue == null) {
			queue = new ArrayBlockingQueue<>(TWEET_QUEUE_SIZE);
			socketWorkerThread = new Thread(new SocketWorker(queue, port), "SocketWorker");
			socketWorkerThread.start();
		}

		if (now - this.lastStatisticLogged >= LOG_STATS) {
			logAndResetStats();
		}

		JsonNode ret = queue.poll(this.inputTimeout, TimeUnit.MILLISECONDS);

		if (ret == null || System.currentTimeMillis() - now >= this.inputTimeout) {
			LOG.info(String.format("Tweet source finished after waiting %.1fs.",
					(System.currentTimeMillis() - now) / 1000.0));
			return null;
		}

		this.tweetsEmitted++;

		return ret;
	}

	private void logAndResetStats() {
		double phase = (System.currentTimeMillis() - this.lastStatisticLogged) / 1000.0;

		LOG.info(String.format("Emitted %.1f tweets/sec for %.1f sec (%d tweets total).",
				this.tweetsEmitted / phase, phase, this.tweetsEmitted));

		this.tweetsEmitted = 0;
		this.lastStatisticLogged = System.currentTimeMillis();
	}

	@Override
	public void shutdown() {
		LOG.info("Got shutdown signal! Interrupting socket worker.");
		logAndResetStats();
		queue = null;
		socketWorkerThread.interrupt();
		socketWorkerThread = null;
	}
}