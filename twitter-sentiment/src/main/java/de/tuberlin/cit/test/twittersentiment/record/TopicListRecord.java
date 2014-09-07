package de.tuberlin.cit.test.twittersentiment.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TopicListRecord extends AbstractTaggableRecord {
	private Map<String, Integer> map;

	public TopicListRecord() {
		map = new LinkedHashMap<>();
	}

	public TopicListRecord(Map<String, Integer> map) {
		this.map = map;
	}

	public Map<String, Integer> getMap() {
		return map;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(map.entrySet().size());
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		int size = in.readInt();
		map = new LinkedHashMap<>(size);
		for (int i = 0; i < size; i++) {
			String key = in.readUTF();
			int value = in.readInt();
			map.put(key, value);
		}
	}
}
