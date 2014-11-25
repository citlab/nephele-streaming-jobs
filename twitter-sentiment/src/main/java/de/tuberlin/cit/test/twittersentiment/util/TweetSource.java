package de.tuberlin.cit.test.twittersentiment.util;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class TweetSource {

	public abstract JsonNode getTweet() throws InterruptedException, IOException;

	public void shutdown() {};
}
