package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.TopicListRecord;
import de.tuberlin.cit.test.twittersentiment.util.Utils;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HotTopicsMergerTask extends IocTask {
	public static final String TIMEOUT = "topicsmerger.timeout";
	public static final int DEFAULT_TIMEOUT = 200;
	private Date lastSent = new Date();
	private int timeout;
	private List<Map<String, Integer>> topicLists = new ArrayList<Map<String, Integer>>();

	@Override
	protected void setup() {
		initReader(0, TopicListRecord.class);
		initWriter(0, TopicListRecord.class);

		timeout = this.getTaskConfiguration().getInteger(TIMEOUT, DEFAULT_TIMEOUT);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void mergeTopicLists(TopicListRecord record, Collector<TopicListRecord> out) {
		topicLists.add(record.getMap());

		Date now = new Date();
		if (now.getTime() - lastSent.getTime() > timeout) {
			Map<String, Integer> averageTopicList = getAverageTopicList();
			out.collect(new TopicListRecord(averageTopicList));
			lastSent = now;
			printRanking(averageTopicList);
			topicLists.clear();
		}
	}

	private Map<String, Integer> getAverageTopicList() {
		Set<String> keys = new HashSet<String>();
		Map<String, Integer> averageTopicList = new HashMap<String, Integer>();

		for (Map<String, Integer> topicList : topicLists) {
			keys.addAll(topicList.keySet());
		}

		for (String key : keys) {
			int sum = 0;
			for (Map<String, Integer> topicList : topicLists) {
				Integer count = topicList.get(key);
				if (count != null) {
					sum += count;
				}
			}
			averageTopicList.put(key, sum / topicLists.size());
		}

		averageTopicList = Utils.sortMapByEntry(averageTopicList,
				new Comparator<Map.Entry<String, Integer>>() {
					public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
						return -(o1.getValue()).compareTo(o2.getValue());
					}
				});
		averageTopicList = Utils.slice(averageTopicList, topicLists.get(0).size());

		return averageTopicList;
	}

	private void printRanking(Map<String, Integer> sortedHashtagCount) {
		System.out.println();
		System.out.println("Average Hashtag Ranking (from " + topicLists.size() +  " topic lists)");
		for (Map.Entry<String, Integer> stringIntegerEntry : sortedHashtagCount.entrySet()) {
			System.out.printf("%s (%d)\n", stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
		}
	}

	@Override
	public int getMaximumNumberOfSubtasks() {
		return 1;
	}
}
