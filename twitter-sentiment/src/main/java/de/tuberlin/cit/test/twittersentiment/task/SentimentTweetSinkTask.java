package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.SentimentTweetRecord;
import de.tuberlin.cit.test.twittersentiment.util.LatencyLog;
import de.tuberlin.cit.test.twittersentiment.util.SentimentTweetLogger;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractOutputTask;

/**
 * Created by bjoern on 4/4/15.
 */
public class SentimentTweetSinkTask extends AbstractOutputTask {

	public final static String LATENCY_LOG_KEY = "sentimenttweettask.latency.log";
	public final static String LATENCY_LOG_DEFAULT = "latency.stat";

	public static final String SENTIMENT_TWEET_LOG_KEY = "sentimenttweettask.tweet.log";
	public static final String SENTIMENT_TWEET_LOG_DEFAULT = "sentimentTweet.dat";

	private RecordReader<SentimentTweetRecord> in;
	private SentimentTweetLogger tweetLogger;

	@Override
	public void registerInputOutput() {
		this.in = new RecordReader<>(this, SentimentTweetRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		LatencyLog latLogger = null;
		try {
			latLogger = new LatencyLog(getTaskConfiguration().getString(
							LATENCY_LOG_KEY, LATENCY_LOG_DEFAULT));
			tweetLogger = new SentimentTweetLogger(getTaskConfiguration().getString(
							SENTIMENT_TWEET_LOG_KEY, SENTIMENT_TWEET_LOG_DEFAULT));


			while (this.in.hasNext()) {
				SentimentTweetRecord record = this.in.next();
				latLogger.log(record.getSrcTimestamp());
				tweetLogger.log(record);
			}
		} finally {
			if (latLogger != null) {
				latLogger.close();
			}
			if(tweetLogger != null) {
				tweetLogger.close();
			}
		}
	}
}
