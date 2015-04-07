package de.tuberlin.cit.test.twittersentiment.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;

public class TimeBasedTweetSource extends TweetSource {

	private static final int TWEET_QUEUE_SIZE = 10000;
	private static final Logger LOG = Logger.getLogger(TimeBasedTweetSource.class);
	private static final long LOG_STATS = 10 * 1000; // log statistics every 10s

	/**
	 * Multiply tweet creation time (in seconds) by given factor.
	 */
	public static final String FACTOR_KEY = "timeBasedTweetSource.factor";
	public static final double DEFAULT_FACTOR = 0.001; // play 1000 original tweet seconds in 1s
	private final double factor;

	/**
	 * Drop tweets if we miss the timeline.
	 */
	public static final String DROP_TWEETS_KEY = "timeBasedTweetSource.dropTweets";
	public static final boolean DEFAULT_DROP_TWEETS = false;
	private final boolean dropTweets;

	/**
	 * Drop tweet if we missed expected send time by given timeout in ms.
	 */
	public static final String DROP_TIMEOUT_KEY = "timeBasedTweetSource.dropTimeout";
	public static final long DEFAULT_DROP_TIMEOUT = 2000; // 2s
	private final long dropTimeout;

	/**
	 * Wait ...ms for input or finish (return null as tweet).
	 */
	public static final String INPUT_TIMEOUT_KEY = "timeBasedTweetSource.inputTimeout";
	public static final long DEFAULT_INPUT_TIMEOUT = 2 * 60 * 1000; // 2min
	private final long inputTimeout;

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZ yyyy", Locale.ENGLISH);

	private long tweetsEmitted;

	private long tweetsDropped;

	private long lastStatisticLogged;

	private long tweetOrigin;

	private long jobOrigin;

	private long lastTweetCreationTimestamp;

	private BlockingQueue<JsonNode> queue;

	private Thread socketWorkerThread;

	private int port;

	public TimeBasedTweetSource(Configuration taskConfiguration, int port) {
		this.factor = taskConfiguration.getDouble(FACTOR_KEY, DEFAULT_FACTOR);
		this.dropTweets = taskConfiguration.getBoolean(DROP_TWEETS_KEY, DEFAULT_DROP_TWEETS);
		this.dropTimeout = taskConfiguration.getLong(DROP_TIMEOUT_KEY, DEFAULT_DROP_TIMEOUT);
		this.inputTimeout = taskConfiguration.getLong(INPUT_TIMEOUT_KEY, DEFAULT_INPUT_TIMEOUT);

		this.tweetsEmitted = 0;
		this.tweetsDropped = 0;
		this.lastStatisticLogged = System.currentTimeMillis();

		this.tweetOrigin = -1;
		this.jobOrigin = -1;

		this.port = port;
	}

	public JsonNode getTweet()
					throws InterruptedException, IOException {

		if (queue == null) {
			queue = new ArrayBlockingQueue<>(TWEET_QUEUE_SIZE);
			socketWorkerThread = new Thread(new SocketWorker(queue, port), "SocketWorker");
			socketWorkerThread.start();
		}

		while (true) {
			long now = System.currentTimeMillis();

			if (System.currentTimeMillis() - this.lastStatisticLogged >= LOG_STATS) {
				logAndResetStats();
			}

			JsonNode ret = queue.poll(this.inputTimeout, TimeUnit.MILLISECONDS);

			if (ret == null || System.currentTimeMillis() - now >= this.inputTimeout) {
				LOG.info(String.format("Tweet source finished after waiting %.1fs.",
								(System.currentTimeMillis() - now) / 1000.0));
				return null;
			}

			try {
				lastTweetCreationTimestamp = this.dateFormat.parse(ret.get("created_at").asText()).getTime();
			} catch (NumberFormatException | ParseException e) {
				LOG.error("Can't parse tweet timestamp: " + ret.get("created_at").asText(), e);
				this.tweetsDropped++;
				continue;
			}

			if (this.tweetOrigin == -1) {
				this.tweetOrigin = lastTweetCreationTimestamp;
				this.jobOrigin = now;
			}

			long nowOriginOffset = now - this.jobOrigin;
			long tweetOriginOffset = lastTweetCreationTimestamp - this.tweetOrigin;
			long sleepFromOrigin = (long) (tweetOriginOffset * this.factor);
			long sleepFromNow = sleepFromOrigin - nowOriginOffset;


			if (this.dropTweets && sleepFromNow < 0 && -1 * sleepFromNow > this.dropTimeout) {
				this.tweetsDropped++;
				continue;
			} else {
				this.tweetsEmitted++;
			}

			if (sleepFromNow > 0) {
				Thread.sleep(sleepFromNow);
			}

			return ret;
		}
	}

	private void logAndResetStats() {
		double phase = (System.currentTimeMillis() - this.lastStatisticLogged) / 1000.0;

		LOG.info(String.format("tw-b: %d;%d;%.1f",
						System.currentTimeMillis() / 1000,
						lastTweetCreationTimestamp / 1000,
						this.tweetsEmitted / phase));
		LOG.info(String.format("Emitted %.1f tweets/sec for %.1f sec (total=%d, emitted=%d, dropped=%d).",
						this.tweetsEmitted / phase,
						phase,
						this.tweetsEmitted + this.tweetsDropped,
						this.tweetsEmitted,
						this.tweetsDropped));

		this.tweetsEmitted = 0;
		this.tweetsDropped = 0;
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