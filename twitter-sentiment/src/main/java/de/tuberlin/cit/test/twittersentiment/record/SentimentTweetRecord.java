package de.tuberlin.cit.test.twittersentiment.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by bjoern on 4/4/15.
 */
public class SentimentTweetRecord extends AbstractTaggableRecord {

	private String tweetId;
	private String tweetText;
	private String tweetSentiment;
	private long srcTimestamp;

	public SentimentTweetRecord() {
	}

	public SentimentTweetRecord(String tweetId, String tweetText, String tweetSentiment, long srcTimestamp) {
		this.tweetId = tweetId;
		this.tweetText = tweetText;
		this.tweetSentiment = tweetSentiment;
		this.srcTimestamp = srcTimestamp;
	}

	public String getTweetId() {
		return tweetId;
	}

	public String getTweetText() {
		return tweetText;
	}

	public String getTweetSentiment() {
		return tweetSentiment;
	}

	public long getSrcTimestamp() {
		return srcTimestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(tweetId);
		out.writeUTF(tweetText);
		out.writeUTF(tweetSentiment);
		out.writeLong(srcTimestamp);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		tweetId = in.readUTF();
		tweetText = in.readUTF();
		tweetSentiment = in.readUTF();
		srcTimestamp = in.readLong();
	}
}
