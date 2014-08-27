package de.tuberlin.cit.test.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tuberlin.cit.test.record.JsonNodeRecord;
import de.tuberlin.cit.test.record.StringRecord;
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
		out1.collect(jsonNodeRecord);
		out2.collect(jsonNodeRecord);
	}
}
