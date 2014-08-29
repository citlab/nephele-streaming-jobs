package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.record.StringRecord;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;

public class JsonConverterTask extends IocTask {
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	protected void setup() {
		initReader(0, StringRecord.class);
		initWriter(0, JsonNodeRecord.class);
		initWriter(1, JsonNodeRecord.class);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = {0, 1})
	public void convertToJson(StringRecord record, Collector<JsonNodeRecord> out1, Collector<JsonNodeRecord> out2) throws IOException {
		JsonNodeRecord jsonNodeRecord = new JsonNodeRecord(objectMapper.readValue(record.toString(), JsonNode.class));

		JsonNode tweet = jsonNodeRecord.getJsonNode();

		// strip unnecessary information
		ObjectNode filteredTweet = new ObjectMapper().createObjectNode();
		String[] includeProperties = {"id",  "text", "lang", "entities"};
		for (String property : includeProperties) {
			filteredTweet.set(property, tweet.get(property));
		}

		jsonNodeRecord = new JsonNodeRecord(filteredTweet);

		out1.collect(jsonNodeRecord);
		out2.collect(jsonNodeRecord);
	}
}
