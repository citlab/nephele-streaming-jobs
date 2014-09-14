package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;
import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.util.TweetSource;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

public class TweetSourceTask extends AbstractGenericInputTask {
	public static final String TCP_SERVER_PORT = "tweetsourcetask.tcpserverport";
	public static final int DEFAULT_TCP_SERVER_PORT = 9001;
	public static final String PROFILE = "tweetsourcetask.profile";

	private RecordWriter<JsonNodeRecord> out1;
	private RecordWriter<JsonNodeRecord> out2;

	private TweetSource source;

	@Override
	public void registerInputOutput() {
		out1 = new RecordWriter<>(this, JsonNodeRecord.class);
		out2 = new RecordWriter<>(this, JsonNodeRecord.class);

		int port = getTaskConfiguration().getInteger(TCP_SERVER_PORT, DEFAULT_TCP_SERVER_PORT);
		String profileName = getTaskConfiguration().getString(PROFILE, null);
		TwitterSentimentJobProfile profile = TwitterSentimentJobProfile.PROFILES.get(profileName);
		source = new TweetSource(profile.loadGenProfile, port);
	}

	@Override
	public void invoke() throws Exception {
		JsonNode tweet;
		while ((tweet = source.getTweet()) != null) {
			out1.emit(new JsonNodeRecord(tweet));
			out2.emit(new JsonNodeRecord(tweet));
		}
	}
}
