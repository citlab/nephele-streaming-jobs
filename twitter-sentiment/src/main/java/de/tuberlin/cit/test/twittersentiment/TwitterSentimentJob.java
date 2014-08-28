package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.task.FileLineWriter;
import de.tuberlin.cit.test.twittersentiment.task.FilterTask;
import de.tuberlin.cit.test.twittersentiment.task.HotTopicsRecognitionTask;
import de.tuberlin.cit.test.twittersentiment.task.JsonConverterTask;
import de.tuberlin.cit.test.twittersentiment.task.SentimentAnalysisTask;
import de.tuberlin.cit.test.twittersentiment.task.SimpleNetworkStreamSourceTask;
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

public class TwitterSentimentJob {
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

		final JobInputVertex networkInput = new JobInputVertex("network", jobGraph);
		networkInput.setInputClass(SimpleNetworkStreamSourceTask.class);
		networkInput.setNumberOfSubtasks(1);
		networkInput.setNumberOfSubtasksPerInstance(1);

		final JobTaskVertex jsonConverterTask = new JobTaskVertex("json", jobGraph);
		jsonConverterTask.setTaskClass(JsonConverterTask.class);
		jsonConverterTask.setNumberOfSubtasks(1);
		jsonConverterTask.setNumberOfSubtasksPerInstance(1);

		final JobTaskVertex hotTopicsTask = new JobTaskVertex("hot topics", jobGraph);
		hotTopicsTask.setTaskClass(HotTopicsRecognitionTask.class);
		hotTopicsTask.setNumberOfSubtasks(16);
		hotTopicsTask.setNumberOfSubtasksPerInstance(4);

		final JobTaskVertex filterTask = new JobTaskVertex("filter", jobGraph);
		filterTask.setTaskClass(FilterTask.class);
		filterTask.setNumberOfSubtasks(16);
		filterTask.setNumberOfSubtasksPerInstance(4);

		final JobTaskVertex sentimentAnalysisTask = new JobTaskVertex("sentiment", jobGraph);
		sentimentAnalysisTask.setTaskClass(SentimentAnalysisTask.class);
		sentimentAnalysisTask.setNumberOfSubtasks(4);
		sentimentAnalysisTask.setNumberOfSubtasksPerInstance(4);


		final JobFileOutputVertex output = new JobFileOutputVertex("output", jobGraph);
		output.setFileOutputClass(FileLineWriter.class);
		output.setFilePath(new Path("file://" + outputPath));
		output.setNumberOfSubtasks(1);
		output.setNumberOfSubtasksPerInstance(1);

		try {

			networkInput.connectTo(jsonConverterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			jsonConverterTask.connectTo(hotTopicsTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			jsonConverterTask.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			hotTopicsTask.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.POINTWISE);
			filterTask.connectTo(sentimentAnalysisTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			sentimentAnalysisTask.connectTo(output, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

			ConstraintUtil.defineAllLatencyConstraintsBetween(networkInput, output, 100);

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

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
