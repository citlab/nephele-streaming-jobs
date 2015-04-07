package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import de.tuberlin.cit.test.twittersentiment.task.*;
import de.tuberlin.cit.test.twittersentiment.util.TimeBasedTweetSource;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class TwitterSentimentTimeMachineJob {
	public static void main(String[] args) {

		CommandLine cli = CliHelper.parseArgs(args);

		String jmHost = cli.getOptionValue("jmaddress").split(":")[0];
		int jmPort = Integer.parseInt(cli.getOptionValue("jmaddress").split(":")[1]);

		TwitterSentimentJobProfile profile = TwitterSentimentJobProfile.PROFILES.get(cli.getOptionValue("profile"));
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", cli.getOptionValue("profile"));
			CliHelper.printUsage();
			System.exit(1);
			return;
		}

		int htMin = Integer.parseInt(cli.getOptionValue("htDop").split(":")[0]);
		int htInitial = Integer.parseInt(cli.getOptionValue("htDop").split(":")[1]);
		int htMax = Integer.parseInt(cli.getOptionValue("htDop").split(":")[2]);

		int filterMin = Integer.parseInt(cli.getOptionValue("filterDop").split(":")[0]);
		int filterInitial = Integer.parseInt(cli.getOptionValue("filterDop").split(":")[1]);
		int filterMax = Integer.parseInt(cli.getOptionValue("filterDop").split(":")[2]);

		int sentimentMin = Integer.parseInt(cli.getOptionValue("sentimentDop").split(":")[0]);
		int sentimentInitial = Integer.parseInt(cli.getOptionValue("sentimentDop").split(":")[1]);
		int sentimentMax = Integer.parseInt(cli.getOptionValue("sentimentDop").split(":")[2]);

		int constraint1 = 15;
		if(cli.hasOption("constraint1")) {
			constraint1 = Integer.parseInt(cli.getOptionValue("constraint1"));
		}

		int constraint2 = 30;
		if(cli.hasOption("constraint2")) {
			constraint2 = Integer.parseInt(cli.getOptionValue("constraint2"));
		}

		double factor = 0.005;
		if(cli.hasOption("factor")) {
			factor = Double.parseDouble(cli.getOptionValue("factor"));
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
			hotTopicsTask.setElasticNumberOfSubtasks(htMin, htMax, htInitial);
			hotTopicsTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.hotTopicsRecognition.subtasksPerInstance);

			final JobTaskVertex topicsMergerTask = new JobTaskVertex("merger", jobGraph);
			topicsMergerTask.setTaskClass(HotTopicsMergerTask.class);
			topicsMergerTask.setNumberOfSubtasks(1);
			topicsMergerTask.setNumberOfSubtasksPerInstance(1);

			final JobTaskVertex filterTask = new JobTaskVertex("filter", jobGraph);
			filterTask.setTaskClass(FilterTask.class);
			filterTask.setElasticNumberOfSubtasks(filterMin, filterMax, filterInitial);
			filterTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.filter.subtasksPerInstance);
			filterTask.getConfiguration().setString(FilterTask.LATENCY_LOG_KEY, cli.getOptionValue("latencyLogFilter"));

			final JobTaskVertex sentimentAnalysisTask = new JobTaskVertex("sentiment", jobGraph);
			sentimentAnalysisTask.setTaskClass(SentimentAnalysisTask.class);
			sentimentAnalysisTask.setElasticNumberOfSubtasks(sentimentMin, sentimentMax, sentimentInitial);
			sentimentAnalysisTask.setNumberOfSubtasksPerInstance(
					profile.paraProfile.filter.subtasksPerInstance);

			final JobOutputVertex output = new JobOutputVertex("sink", jobGraph);
			output.setOutputClass(SentimentTweetSinkTask.class);
			output.getConfiguration().setString(SentimentTweetSinkTask.LATENCY_LOG_KEY, cli.getOptionValue("latencyLogSink"));
			output.getConfiguration().setString(SentimentTweetSinkTask.SENTIMENT_TWEET_LOG_KEY, cli.getOptionValue("tweetLog"));
			output.setNumberOfSubtasks(1);
			output.setNumberOfSubtasksPerInstance(1);

			input.connectTo(hotTopicsTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			input.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			hotTopicsTask.connectTo(topicsMergerTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			topicsMergerTask.connectTo(filterTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			filterTask.connectTo(sentimentAnalysisTask, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
			sentimentAnalysisTask.connectTo(output, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

			ConstraintUtil.defineAllLatencyConstraintsBetween(input.getForwardConnection(1),
					sentimentAnalysisTask.getForwardConnection(0), constraint1);
			ConstraintUtil.defineAllLatencyConstraintsBetween(
					input.getForwardConnection(0),
					topicsMergerTask.getForwardConnection(0), constraint2);

			// enable chaining
//			filterTask.setVertexToShareInstancesWith(sentimentAnalysisTask);

			// enable local testing
//			input.setVertexToShareInstancesWith(hotTopicsTask);
//			hotTopicsTask.setVertexToShareInstancesWith(topicsMergerTask);
//			topicsMergerTask.setVertexToShareInstancesWith(filterTask);
//			sentimentAnalysisTask.setVertexToShareInstancesWith(output);

			addJars(cli, jobGraph);

			Configuration conf = new Configuration();
			conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jmHost);
			conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jmPort);

			final JobClient jobClient = new JobClient(jobGraph, conf);
			jobClient.submitJobAndWait();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void addJars(CommandLine cli, JobGraph graph) throws IOException, InterruptedException {
		if (cli.hasOption("libdir")) {
			File libdir = new File(cli.getOptionValue("libdir"));

			if (!libdir.exists() || !libdir.isDirectory()) {
				System.out.println(cli.getOptionValue("libdir") + " is not valid library directory");
				System.exit(1);
			}

			File[] jarfiles = libdir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".jar");
				}
			});

			for (File jarfile : jarfiles) {
				graph.addJar(new Path(jarfile.getAbsolutePath()));
			}
		} else {
			compileMaven();
			graph.addJar(new Path("target/twitter-sentiment-git-jar-with-dependencies.jar"));
		}
	}

	private static void compileMaven() throws IOException, InterruptedException {
		// Create jar file for job deployment
		Process p = Runtime.getRuntime().exec("mvn clean package");
		if (p.waitFor() != 0) {
			System.out.println("Failed to build test-queue-behavior.jar");
			System.exit(1);
		}
	}


}
