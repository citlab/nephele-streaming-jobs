package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.record.TopicListRecord;
import de.tuberlin.cit.test.twittersentiment.util.Utils;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class HotTopicsRecognitionTask extends IocTask {
	public static final String HISTORY_SIZE = "hottopicsrecognition.historysize";
	public static final int DEFAULT_HISTORY_SIZE = 50000;
	public static final String TOP_COUNT = "hottopicsrecognition.topcount";
	public static final int DEFAULT_TOP_COUNT = 25;
	public static final String TIMEOUT = "hottopicsrecognition.timeout";
	public static final int DEFAULT_TIMEOUT = 100;

	private Date lastSent = new Date();
	private int timeout;
	private Queue<ArrayNode> hashtagHistory;
	private Map<String, Integer> hashtagCount;
	private int topCount;
	private int historySize;
	private int historyWarmupCount = 0;

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initWriter(0, TopicListRecord.class);

		historySize = this.getTaskConfiguration().getInteger(HISTORY_SIZE, DEFAULT_HISTORY_SIZE);
		topCount = this.getTaskConfiguration().getInteger(TOP_COUNT, DEFAULT_TOP_COUNT);
		timeout = this.getTaskConfiguration().getInteger(TIMEOUT, DEFAULT_TIMEOUT);

		hashtagHistory = new ArrayDeque<>(historySize);
		hashtagCount  = new HashMap<>(topCount);

		// fill dummy history
		ArrayNode dummy = new ObjectMapper().createArrayNode();
		for (int i = 0; i < historySize; i++) {
			hashtagHistory.offer(dummy);
		}

	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void recognizeHotTopics(JsonNodeRecord record, Collector<TopicListRecord> out) {
		JsonNode jsonNode = record.getJsonNode();
		ArrayNode hashtags = (ArrayNode) jsonNode.get("entities").get("hashtags");

		if (hashtags.size() != 0) {
			// forget history
			ArrayNode oldHashtags = hashtagHistory.poll();
			for (JsonNode hashtag : oldHashtags) {
				String text = hashtag.get("text").asText().toLowerCase();
				Integer count = hashtagCount.get(text);
				if (count == null) {
					continue;
				}
				if (count <= 1) {
					hashtagCount.remove(text);
				} else {
					hashtagCount.put(text, count - 1);
				}
			}

			// update hashtag count
			for (JsonNode hashtag : hashtags) {
				String text = hashtag.get("text").asText().toLowerCase();
				Integer count = hashtagCount.get(text);
				if (count == null) {
					count = 0;
				}
				hashtagCount.put(text, count + 1);
			}
			hashtagHistory.offer(hashtags);
			if (historyWarmupCount < historySize) {
				++historyWarmupCount;
			}
		}


		// send one topic list every interval
		Date now = new Date();
		if (now.getTime() - lastSent.getTime() > timeout && historyWarmupCount == historySize) {
			// construct topic list
			Map<String, Integer> sortedHashtagCount = Utils.sortMapByEntry(hashtagCount,
					new Comparator<Map.Entry<String, Integer>>() {
						public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
							return -(o1.getValue()).compareTo(o2.getValue());
						}
					});
			Map<String, Integer> topicList = Utils.slice(sortedHashtagCount, topCount);
			out.collect(new TopicListRecord(topicList));
			lastSent = now;
		}
	}
}
