package de.tuberlin.cit.test.task;

import com.fasterxml.jackson.databind.JsonNode;
import de.tuberlin.cit.test.record.JsonNodeRecord;
import de.tuberlin.cit.test.record.StringRecord;
import de.tuberlin.cit.test.util.SentimentClassifier;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

import java.io.IOException;

public class SentimentAnalysisTask extends IocTask {
	private SentimentClassifier sentimentClassifier = new SentimentClassifier();

	@Override
	protected void setup() {
		initReader(0, JsonNodeRecord.class);
		initWriter(0, StringRecord.class);
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void analyzeSentiment(JsonNodeRecord record, Collector<StringRecord> out) throws IOException, InterruptedException {
		JsonNode jsonNode = record.getJsonNode();
		String tweetText = jsonNode.get("text").asText();
		String tweetId = jsonNode.get("id").asText();

		String sentiment = sentimentClassifier.classify(tweetText);
		out.collect(new StringRecord(tweetId + ";" + sentiment + ";" + tweetText));
	}
}
