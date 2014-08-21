package de.tuberlin.cit.test.task;

import com.fasterxml.jackson.databind.JsonNode;
import de.tuberlin.cit.test.record.JsonNodeRecord;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;

public class FilterTask extends IocTask {

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initWriter(0, JsonNodeRecord.class);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void filterTweet(JsonNodeRecord record, Collector<JsonNodeRecord> out) throws IOException {
		JsonNode jsonNode = record.getJsonNode();
		String lang = jsonNode.get("lang").asText();
		if (lang.equals("en")) {
			out.collect(record);
		}
	}
}
