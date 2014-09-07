package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.task.FileLineWriter;
import de.tuberlin.cit.test.twittersentiment.task.FilterTask;
import de.tuberlin.cit.test.twittersentiment.task.HotTopicsMergerTask;
import de.tuberlin.cit.test.twittersentiment.task.HotTopicsRecognitionTask;
import de.tuberlin.cit.test.twittersentiment.task.SentimentAnalysisTask;
import de.tuberlin.cit.test.twittersentiment.task.TweetSourceTask;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;

import java.io.IOException;

public class TwitterSentimentLocalJob {
	public static void main(String[] args) {
		if (args.length != 2) {
			printUsage();
			System.exit(1);
			return;
		}

		String jmHost = args[0].split(":")[0];
		int jmPort = Integer.parseInt(args[0].split(":")[1]);
		String outputPath = args[1];

		JobGraph jobGraph = new JobGraph("twitter sentiment");

		final JobInputVertex input = new JobInputVertex("input", jobGraph);
		input.setInputClass(TweetSourceTask.class);
		input.getConfiguration().setString(TweetSourceTask.PROFILE, "local_dualcore");
		input.setNumberOfSubtasks(1);

		final JobTaskVertex hotTopicsTask = new JobTaskVertex("hot topics", jobGraph);
		hotTopicsTask.setTaskClass(HotTopicsRecognitionTask.class);
		hotTopicsTask.setNumberOfSubtasks(1);
		hotTopicsTask.getConfiguration().setInteger(HotTopicsRecognitionTask.HISTORY_SIZE, 1000);

		final JobTaskVertex topicsMergerTask = new JobTaskVertex("merger", jobGraph);
		topicsMergerTask.setTaskClass(HotTopicsMergerTask.class);
		topicsMergerTask.setNumberOfSubtasks(1);

		final JobTaskVertex filterTask = new JobTaskVertex("filter", jobGraph);
		filterTask.setTaskClass(FilterTask.class);
		filterTask.setNumberOfSubtasks(1);

		final JobTaskVertex sentimentAnalysisTask = new JobTaskVertex("sentiment", jobGraph);
		sentimentAnalysisTask.setTaskClass(SentimentAnalysisTask.class);
		sentimentAnalysisTask.setNumberOfSubtasks(1);

		final JobFileOutputVertex output = new JobFileOutputVertex("output", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path("file://" + outputPath));
		output.setNumberOfSubtasks(1);


		input.setVertexToShareInstancesWith(hotTopicsTask);
		hotTopicsTask.setVertexToShareInstancesWith(topicsMergerTask);
		topicsMergerTask.setVertexToShareInstancesWith(filterTask);
		filterTask.setVertexToShareInstancesWith(sentimentAnalysisTask);
		sentimentAnalysisTask.setVertexToShareInstancesWith(output);

		try {

			input.connectTo(hotTopicsTask, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);
			input.connectTo(filterTask, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);
			hotTopicsTask.connectTo(topicsMergerTask, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);
			topicsMergerTask.connectTo(filterTask, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);
			filterTask.connectTo(sentimentAnalysisTask, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
			sentimentAnalysisTask.connectTo(output, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);

			ConstraintUtil.defineAllLatencyConstraintsBetween(input, output, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// Create jar file for job deployment
		try {
			Process p = Runtime.getRuntime().exec("mvn clean package");

			if (p.waitFor() != 0) {
				System.out.println("Failed to build twitter-sentiment-git.jar");
				System.exit(1);
			}

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}


//		jobGraph.addJar(new Path("target/twitter-sentiment-git.jar"));
		jobGraph.addJar(new Path("target/twitter-sentiment-git-jar-with-dependencies.jar"));

		Configuration conf = new Configuration();
		conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jmHost);
		conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jmPort);


		try {
			final JobClient jobClient = new JobClient(jobGraph, conf);
			jobClient.submitJobAndWait();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printUsage() {
		System.err.println("Parameters: <jobmanager-host>:<port> <output-file>");
	}
}
