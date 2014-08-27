package de.tuberlin.cit.test.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.tuberlin.cit.test.record.JsonNodeRecord;
import de.tuberlin.cit.test.record.StringListRecord;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterTask extends IocTask {
	private List<String> topics = new ArrayList<String>();

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initReader(1, StringListRecord.class);
		initWriter(0, JsonNodeRecord.class);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void filterTweet(JsonNodeRecord record, Collector<JsonNodeRecord> out) throws IOException {
		JsonNode jsonNode = record.getJsonNode();

		// actually obsolete now that the tweets are already filtered
		String lang = jsonNode.get("lang").asText();
		if (!lang.equals("en")) {
			return;
		}

		if (topics.size() == 0) {
			out.collect(record);
			return;
		}

		ArrayNode hashtags = (ArrayNode) jsonNode.get("entities").get("hashtags");
		for (JsonNode hashtag : hashtags) {
			if (topics.contains(hashtag.get("text").asText().toLowerCase())) {
				out.collect(record);
			}
		}
	}

	@ReadFromWriteTo(readerIndex = 1)
	public void updateTopicList(StringListRecord record) {
		topics = record.getList();
	}
}
