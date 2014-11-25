package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import de.tuberlin.cit.test.twittersentiment.task.FilterTask;
import de.tuberlin.cit.test.twittersentiment.task.HotTopicsMergerTask;
import de.tuberlin.cit.test.twittersentiment.task.HotTopicsRecognitionTask;
import de.tuberlin.cit.test.twittersentiment.task.SentimentAnalysisTask;
import de.tuberlin.cit.test.twittersentiment.task.TweetSourceTask;
import de.tuberlin.cit.test.twittersentiment.util.TimeBasedTweetSource;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;

public class TwitterSentimentTimeMachineJob {
	public static void main(String[] args) {
		if (args.length != 4) {
			printUsage();
			System.exit(1);
			return;
		}

		String jmHost = args[0].split(":")[0];
		int jmPort = Integer.parseInt(args[0].split(":")[1]);
		String outputPath = args[1];
		TwitterSentimentJobProfile profile = TwitterSentimentJobProfile.PROFILES.get(args[2]);
		double factor = Double.parseDouble(args[3]); 

		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[2]);
			printUsage();
			System.exit(1);
			return;
		}

		try {
			JobGraph jobGraph = new JobGraph("twitter sentiment timemachine");

			final JobInputVertex input = new JobInputVertex("input", jobGraph);
			input.setInputClass(TweetSourceTask.class);
			input.getConfiguration().setString(TweetSourceTask.TWEET_SOURCE, TimeBasedTweetSource.class.getSimpleName());
			input.getConfiguration().setDouble(TimeBasedTweetSource.FACTOR_KEY, factor);
			input.getConfiguration().setBoolean(TimeBasedTweetSource.DROP_TWEETS_KEY, false); // drop tweets
			input.getConfiguration().setLong(TimeBasedTweetSource.DROP_TIMEOUT_KEY, 1000); // if we missed the timeline by 1000ms
			input.getConfiguration().setLong(TimeBasedTweetSource.INPUT_TIMEOUT_KEY, 2*60*1000); // finish task after 2min waiting for input
			input.setNumberOfSubtasks(1);
			input.setNumberOfSubtasksPerInstance(1);

			final JobTaskVertex hotTopicsTask = new JobTaskVertex("hot topics", jobGraph);
			hotTopicsTask.setTaskClass(HotTopicsRecognitionTask.class);
			hotTopicsTask.getConfiguration().setInteger(
					HotTopicsRecognitionTask.HISTORY_SIZE,
					profile.paraProfile.hotTopicsRecognition.historySize);
			hotTopicsTask.getConfiguration().setInteger(
					HotTopicsRecognitionTask.TOP_COUNT,
					profile.paraProfile.hotTopicsRecognition.topCount);
			hotTopicsTask.setElasticNumberOfSubtasks(
					profile.paraProfile.hotTopicsRecognition.minimumAmountSubtasks,
					profile.paraProfile.hotTopicsRecognition.maximumAmountSubtasks,
					profile.paraProfile.hotTopicsRecognition.initalAmountSubtasks);
			hotTopicsTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.hotTopicsRecognition.subtasksPerInstance);

			final JobTaskVertex topicsMergerTask = new JobTaskVertex("merger", jobGraph);
			topicsMergerTask.setTaskClass(HotTopicsMergerTask.class);
			topicsMergerTask.setNumberOfSubtasks(1);
			topicsMergerTask.setNumberOfSubtasksPerInstance(1);

			final JobTaskVertex filterTask = new JobTaskVertex("filter", jobGraph);
			filterTask.setTaskClass(FilterTask.class);
			filterTask.setElasticNumberOfSubtasks(
					profile.paraProfile.filter.minimumAmountSubtasks,
					profile.paraProfile.filter.maximumAmountSubtasks,
					profile.paraProfile.filter.initalAmountSubtasks);
			filterTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.filter.subtasksPerInstance);

			final JobTaskVertex sentimentAnalysisTask = new JobTaskVertex("sentiment", jobGraph);
			sentimentAnalysisTask.setTaskClass(SentimentAnalysisTask.class);
			sentimentAnalysisTask.setElasticNumberOfSubtasks(
					profile.paraProfile.sentiment.minimumAmountSubtasks,
					profile.paraProfile.sentiment.maximumAmountSubtasks,
					profile.paraProfile.sentiment.initalAmountSubtasks);
			sentimentAnalysisTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.filter.subtasksPerInstance);

			final JobOutputVertex output = new JobOutputVertex("output", jobGraph);
			output.setOutputClass(FileLineWriter.class);
			output.getConfiguration().setString("outputPath", "file://" + outputPath);
			output.setNumberOfSubtasks(1);
			output.setNumberOfSubtasksPerInstance(1);

			input.connectTo(hotTopicsTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			input.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			hotTopicsTask.connectTo(topicsMergerTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			topicsMergerTask.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			filterTask.connectTo(sentimentAnalysisTask, ChannelType.NETWORK, DistributionPattern.POINTWISE);
			sentimentAnalysisTask.connectTo(output, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

			ConstraintUtil.defineAllLatencyConstraintsBetween(input.getForwardConnection(1),
					sentimentAnalysisTask.getForwardConnection(0), 30);
			ConstraintUtil.defineAllLatencyConstraintsBetween(
					input.getForwardConnection(0),
					topicsMergerTask.getForwardConnection(0), 210);

			// enable chaining
//			filterTask.setVertexToShareInstancesWith(sentimentAnalysisTask);

			// enable local testing
//			input.setVertexToShareInstancesWith(hotTopicsTask);
//			hotTopicsTask.setVertexToShareInstancesWith(topicsMergerTask);
//			topicsMergerTask.setVertexToShareInstancesWith(filterTask);
//			sentimentAnalysisTask.setVertexToShareInstancesWith(output);

			// Create jar file for job deployment
			System.out.println("Running mvn clean package...");
			Process p = Runtime.getRuntime().exec("mvn clean package");
			if (p.waitFor() != 0) {
				System.out.println("Failed to build twitter-sentiment-git.jar");
				System.exit(1);
			}

			System.out.println("Launching job...");
//			jobGraph.addJar(new Path("target/twitter-sentiment-git.jar"));
			jobGraph.addJar(new Path("target/twitter-sentiment-git-jar-with-dependencies.jar"));

			Configuration conf = new Configuration();
			conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jmHost);
			conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jmPort);

			final JobClient jobClient = new JobClient(jobGraph, conf);
			jobClient.submitJobAndWait();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printUsage() {
		System.err
				.println("Parameters: <jobmanager-host>:<port> <output-file> <profile> <factor>");
	}
}
