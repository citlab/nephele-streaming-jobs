package de.tuberlin.cit.test.twittersentiment.util;

import de.tuberlin.cit.test.twittersentiment.record.SentimentTweetRecord;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by bjoern on 4/4/15.
 */
public class SentimentTweetLogger {

	private static final Logger LOG = Logger.getLogger(LatencyLog.class);

	public static final int LOG_INTERVAL_MILLIS = 5000;

	private ExecutorService backgroundWorker = Executors.newSingleThreadExecutor();

	private long nextLogIntervalBegin;

	private StringBuffer strBuf;

	private Writer out;

	public SentimentTweetLogger(String file) throws IOException {
		out = new FileWriter(file);
		out.write("tweetId;tweetText;tweetSentiment\n");
		out.flush();

		strBuf = new StringBuffer();

		nextLogIntervalBegin = alignToInterval(System.currentTimeMillis() + LOG_INTERVAL_MILLIS,
						LOG_INTERVAL_MILLIS);

	}

	public void log(SentimentTweetRecord record) {
		strBuf.append(String.format("%s;%s;%s\n", record.getTweetId(), record.getTweetText(), record.getTweetSentiment()));

		if (record.getSrcTimestamp() >= nextLogIntervalBegin) {
			String logLine = strBuf.toString();
			backgroundWorker.submit(createLogWorker(logLine, out));
			strBuf = new StringBuffer();

			while (nextLogIntervalBegin <= System.currentTimeMillis()) {
				nextLogIntervalBegin += LOG_INTERVAL_MILLIS;
			}
		}
	}

	private static Runnable createLogWorker(final String content, final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					out.write(content);
					out.flush();
				} catch (IOException e) {
					LOG.error("Error when writing to receiver latency log", e);
				}
			}


		};
	}

	private static Runnable createCloseLogWorker(final String content, final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					if (content != null) {
						out.write(content);
					}
					out.close();
				} catch (IOException e) {
					LOG.error("Error when closing receiver latency log", e);
				}
			}
		};
	}

	public void close() {
		if (out != null) {
			if (strBuf.length() > 0) {
				backgroundWorker.submit(createCloseLogWorker(strBuf.toString(), out));
			} else {
				backgroundWorker.submit(createCloseLogWorker(null, out));
			}
			strBuf = null;
			backgroundWorker.shutdown();
			out = null;
		}
	}


	private static long alignToInterval(long timestampInMillis, long interval) {
		long remainder = timestampInMillis % interval;

		return timestampInMillis - remainder;
	}

}
