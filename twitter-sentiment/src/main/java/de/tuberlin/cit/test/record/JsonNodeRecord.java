package de.tuberlin.cit.test.record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JsonNodeRecord extends AbstractTaggableRecord {
	private JsonNode jsonNode;
	private ObjectMapper objectMapper = new ObjectMapper();

	public JsonNodeRecord() {
	}

	public JsonNodeRecord(JsonNode jsonNode) {
		this.jsonNode = jsonNode;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(jsonNode.toString());
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		jsonNode = objectMapper.readValue(in.readUTF(), JsonNode.class);
	}

	public JsonNode getJsonNode() {
		return jsonNode;
	}
}
