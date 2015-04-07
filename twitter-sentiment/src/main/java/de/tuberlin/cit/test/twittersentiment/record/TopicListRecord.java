package de.tuberlin.cit.test.twittersentiment.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TopicListRecord extends AbstractTaggableRecord {
	private int senderId;

	private Map<String, Integer> map;
	private long srcTimestamp;

	public TopicListRecord(long srcTimestamp, int senderId, Map<String, Integer> topicList) {
		this.map = topicList;
		this.srcTimestamp = srcTimestamp;
		this.senderId = senderId;
	}

	public TopicListRecord() {
	}

	public Map<String, Integer> getMap() {
		return map;
	}

	public int getSenderId() {
		return senderId;
	}

	public long getSrcTimestamp() {
		return srcTimestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(senderId);
		out.writeLong(srcTimestamp);
		out.writeInt(map.entrySet().size());
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		senderId = in.readInt();
		srcTimestamp = in.readLong();
		int size = in.readInt();
		map = new LinkedHashMap<>(size);
		for (int i = 0; i < size; i++) {
			String key = in.readUTF();
			int value = in.readInt();
			map.put(key, value);
		}
	}
}
