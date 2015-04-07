package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.record.TopicListRecord;
import de.tuberlin.cit.test.twittersentiment.util.LatencyLog;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FilterTask extends IocTask {

	public final static String LATENCY_LOG_KEY = "filtertask.latency.log";
	public final static String LATENCY_LOG_DEFAULT = "latency-1.stat";

	private Set<String> topics = new HashSet<String>();
	private LatencyLog latencyLog;

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initReader(1, TopicListRecord.class);
		initWriter(0, JsonNodeRecord.class);
		try {
			if (getIndexInSubtaskGroup() == 0) {
				latencyLog = new LatencyLog(getTaskConfiguration().getString(LATENCY_LOG_KEY, LATENCY_LOG_DEFAULT));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void filterTweet(JsonNodeRecord record, Collector<JsonNodeRecord> out) throws IOException {
		JsonNode tweet = record.getJsonNode();

		// actually obsolete now that the tweets are already filtered
		String lang = tweet.get("lang").asText();
		if (!lang.equals("en")) {
			return;
		}

		// check if tweet is tagged with a hot topic
		ArrayNode hashtags = (ArrayNode) tweet.get("entities").get("hashtags");
		for (JsonNode hashtag : hashtags) {
			if (topics.contains(hashtag.get("text").asText().toLowerCase())) {
				out.collect(new JsonNodeRecord(tweet, record.getSrcTimestamp()));
			}
		}
	}

	@ReadFromWriteTo(readerIndex = 1)
	public void updateTopicList(TopicListRecord record) {
		topics = record.getMap().keySet();
		if (latencyLog != null) {
			latencyLog.log(record.getSrcTimestamp());
		}
	}
}
