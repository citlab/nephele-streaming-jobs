package de.tuberlin.cit.test.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.tuberlin.cit.test.record.JsonNodeRecord;
import de.tuberlin.cit.test.record.StringListRecord;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class HotTopicsRecognitionTask extends IocTask {
	private static final int HISTORY_SIZE = 50000;
	private static final int TOP_COUNT = 20;
	private Map<String, Integer> hashtagCount = new HashMap<String, Integer>();
	private Queue<ArrayNode> hashtagHistory = new ArrayDeque<ArrayNode>(HISTORY_SIZE);

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initWriter(0, StringListRecord.class);

		// fill dummy history
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode dataTable = objectMapper.createObjectNode();
		ArrayNode dummy = dataTable.putArray("dummy");
		for (int i = 0; i < HISTORY_SIZE; i++) {
			hashtagHistory.offer(dummy);
		}
	}

	@Override
	protected void shutdown() {
		Map<String, Integer> sortedHashtagCount =
				sortByComparator(hashtagCount, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return -(o1.getValue()).compareTo(o2.getValue());
			}
		});
		printRanking(sortedHashtagCount);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void recognizeHotTopics(JsonNodeRecord record, Collector<StringListRecord> out) {
		JsonNode jsonNode = record.getJsonNode();
		ArrayNode hashtags = (ArrayNode) jsonNode.get("entities").get("hashtags");

		// update hashtag count
		for (JsonNode hashtag : hashtags) {
			String text = hashtag.get("text").asText().toLowerCase();
			Integer count = hashtagCount.get(text);
			if (count == null) {
				count = 0;
			}
			hashtagCount.put(text, count + 1);
		}

		if (hashtags.size() != 0) {
			// forget history
			ArrayNode oldHashtags = hashtagHistory.poll();
			for (JsonNode hashtag : oldHashtags) {
				String text = hashtag.get("text").asText().toLowerCase();
				Integer count = hashtagCount.get(text);
				if (count == null) {
					continue;
				}
				if (count == 1) {
					hashtagCount.remove(text);
				} else {
					hashtagCount.put(text, count - 1);
				}
			}

			hashtagHistory.offer(hashtags);
		}


		// construct topic list
		Map<String, Integer> sortedHashtagCount =
				sortByComparator(hashtagCount, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return -(o1.getValue()).compareTo(o2.getValue());
			}
		});
		List<String> topicList = new ArrayList<String>();
		Iterator<String> iterator = sortedHashtagCount.keySet().iterator();
		for (int i = 0; i < TOP_COUNT; i++) {
			if (!iterator.hasNext()) {
				break;
			}
			topicList.add(iterator.next());
		}
		out.collect(new StringListRecord(topicList));
	}

	private <K, V> Map<K, V> sortByComparator(Map<K, V> unsortedMap, Comparator<Map.Entry<K, V>> comparator) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(unsortedMap.entrySet());

		Collections.sort(list, comparator);

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	private void printRanking(Map<String, Integer> sortedHashtagCount) {
		int i = 0;
		System.out.println("Hashtag Ranking");
		for (Map.Entry<String, Integer> stringIntegerEntry : sortedHashtagCount.entrySet()) {
			System.out.printf("%s (%d)\n", stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
			if (++i == TOP_COUNT) {
				return;
			}
		}
	}
}
