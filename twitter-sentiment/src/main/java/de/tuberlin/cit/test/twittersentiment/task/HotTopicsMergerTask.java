package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.TopicListRecord;
import de.tuberlin.cit.test.twittersentiment.util.Utils;
import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.util.*;

public class HotTopicsMergerTask extends IocTask {

	public static final String TIMEOUT = "topicsmerger.timeout";
	public static final int DEFAULT_TIMEOUT = 200;
	private final Map<Integer, TopicListRecord> topicLists = new HashMap<>();
	private int timeout;
	private int copiesToBroadcast = 1;

	private long nextForwardDeadline;
	private long minSrcTimestamp = Long.MAX_VALUE;
	private long maxSrcTimestamp = Long.MIN_VALUE;


	@Override
	protected void setup() {
		initReader(0, TopicListRecord.class);
		initWriter(0, TopicListRecord.class, new DefaultChannelSelector<TopicListRecord>() {
			@Override
			public int[] selectChannels(TopicListRecord record, int numberOfOutputChannels) {
				copiesToBroadcast = numberOfOutputChannels;
				return super.selectChannels(record, numberOfOutputChannels);
			}
		});

		timeout = this.getTaskConfiguration().getInteger(TIMEOUT, DEFAULT_TIMEOUT);
		nextForwardDeadline = Utils.alignToInterval(System.currentTimeMillis() + timeout, timeout);
		resetTimestamps();
	}

	private void resetTimestamps() {
		minSrcTimestamp = Long.MAX_VALUE;
		maxSrcTimestamp = Long.MIN_VALUE;
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void mergeTopicLists(TopicListRecord record, Collector<TopicListRecord> out) {
		topicLists.put(record.getSenderId(), record);

		minSrcTimestamp = Math.min(minSrcTimestamp, record.getSrcTimestamp());
		maxSrcTimestamp = Math.max(maxSrcTimestamp, record.getSrcTimestamp());

		if (System.currentTimeMillis() >= nextForwardDeadline) {
			if (!topicLists.isEmpty()) {
				TopicListRecord averageTopicList = getMergedTopicList();
				for (int i = 0; i < copiesToBroadcast; i++) {
					out.collect(averageTopicList);
				}

				// printRanking(averageTopicList);
				topicLists.clear();
				resetTimestamps();
			}

			nextForwardDeadline += timeout;
		}
	}

	private TopicListRecord getMergedTopicList() {
		Map<String, Integer> averageTopicList = new HashMap<String, Integer>();

		int topCount = 0;
		for (TopicListRecord topicList : topicLists.values()) {
			for(Map.Entry<String, Integer> entry : topicList.getMap().entrySet()) {
				String hashtag = entry.getKey();
				int newCount = entry.getValue();

				Integer oldCount = averageTopicList.get(hashtag);
				if(oldCount != null) {
					newCount += oldCount;
				}

				averageTopicList.put(hashtag, newCount);
			}
			topCount = Math.max(topCount, topicList.getMap().size());
		}

		averageTopicList = Utils.sortMapByEntry(averageTopicList,
				new Comparator<Map.Entry<String, Integer>>() {
					public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
						return -(o1.getValue()).compareTo(o2.getValue());
					}
				});
		averageTopicList = Utils.slice(averageTopicList, topCount);

		return new TopicListRecord((minSrcTimestamp + maxSrcTimestamp) / 2, 0, averageTopicList);
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
