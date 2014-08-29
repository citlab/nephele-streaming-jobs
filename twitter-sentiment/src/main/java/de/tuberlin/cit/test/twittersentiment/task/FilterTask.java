package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.record.TopicListRecord;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FilterTask extends IocTask {
	private Set<String> topics = new HashSet<String>();

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initReader(1, TopicListRecord.class);
		initWriter(0, JsonNodeRecord.class);
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
				out.collect(new JsonNodeRecord(tweet));
			}
		}
	}

	@ReadFromWriteTo(readerIndex = 1)
	public void updateTopicList(TopicListRecord record) {
		topics = record.getMap().keySet();
	}
}
