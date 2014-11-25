package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;

import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.util.LoadPhaseTweetSource;
import de.tuberlin.cit.test.twittersentiment.util.TimeBasedTweetSource;
import de.tuberlin.cit.test.twittersentiment.util.TweetSource;
import de.tuberlin.cit.test.twittersentiment.util.UnlimitedTweetSource;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

public class TweetSourceTask extends AbstractGenericInputTask {
	public static final String TCP_SERVER_PORT = "tweetsourcetask.tcpserverport";
	public static final int DEFAULT_TCP_SERVER_PORT = 9001;
	public static final String TWEET_SOURCE = "tweetsourcetask.tweetsource";
	public static final String DEFAULT_TWEET_SOURCE = LoadPhaseTweetSource.class.getSimpleName();

	private RecordWriter<JsonNodeRecord> out1;
	private RecordWriter<JsonNodeRecord> out2;

	private TweetSource source;

	@Override
	public void registerInputOutput() {
		out1 = new RecordWriter<>(this, JsonNodeRecord.class);
		out2 = new RecordWriter<>(this, JsonNodeRecord.class);

		int port = getTaskConfiguration().getInteger(TCP_SERVER_PORT, DEFAULT_TCP_SERVER_PORT);
		String sourceName = getTaskConfiguration().getString(TWEET_SOURCE, DEFAULT_TWEET_SOURCE);

		if (sourceName.equals(LoadPhaseTweetSource.class.getSimpleName())) {
			source = new LoadPhaseTweetSource(getTaskConfiguration(), port);
		} else if (sourceName.equals(TimeBasedTweetSource.class.getSimpleName())) {
			source = new TimeBasedTweetSource(getTaskConfiguration(), port);
		} else if (sourceName.equals(UnlimitedTweetSource.class.getSimpleName())) {
			source = new UnlimitedTweetSource(getTaskConfiguration(), port);
		} else {
			throw new RuntimeException("Uknown tweet source.");
		}
	}

	@Override
	public void invoke() throws Exception {
		JsonNode tweet;

		while ((tweet = source.getTweet()) != null) {
			out1.emit(new JsonNodeRecord(tweet));
			out2.emit(new JsonNodeRecord(tweet));
		}

		source.shutdown(); // shutdown input thread and close socket
	}

	@Override
	public void cancel() throws Exception {
		source.shutdown(); // shutdown input thread and close socket
	}
}
