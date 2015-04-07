package de.tuberlin.cit.test.twittersentiment.task;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

import de.tuberlin.cit.test.twittersentiment.record.JsonNodeRecord;
import de.tuberlin.cit.test.twittersentiment.record.SentimentTweetRecord;
import de.tuberlin.cit.test.twittersentiment.util.SentimentClassifier;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;
import eu.stratosphere.nephele.types.StringRecord;

public class SentimentAnalysisTask extends IocTask {
	private SentimentClassifier sentimentClassifier = new SentimentClassifier();

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initWriter(0, SentimentTweetRecord.class);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void analyzeSentiment(JsonNodeRecord record, Collector<SentimentTweetRecord> out)
			throws IOException, InterruptedException {

		JsonNode jsonNode = record.getJsonNode();
		String tweetText = jsonNode.get("text").asText();
		String tweetId = jsonNode.get("id").asText();

		String sentiment = sentimentClassifier.classify(tweetText);
		out.collect(new SentimentTweetRecord(tweetId, tweetText, sentiment, record.getSrcTimestamp()));
	}
}
