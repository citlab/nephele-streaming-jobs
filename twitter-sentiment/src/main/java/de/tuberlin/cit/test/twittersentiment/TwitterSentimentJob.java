package de.tuberlin.cit.test.twittersentiment;

import java.io.IOException;

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
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;

public class TwitterSentimentJob {
	public static void main(String[] args) {
		
		
		if (args.length != 3) {
			printUsage();
			System.exit(1);
			return;
		}

		String jmHost = args[0].split(":")[0];
		int jmPort = Integer.parseInt(args[0].split(":")[1]);
		
		String outputPath = args[1];
		
		TwitterSentimentJobProfile profile = TwitterSentimentJobProfile.PROFILES.get(args[2]);
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[2]);
			printUsage();
			System.exit(1);
			return;			
		}
		
		try {
			JobGraph jobGraph = new JobGraph("twitter sentiment");
	
			final JobInputVertex input = new JobInputVertex("input", jobGraph);
			input.setInputClass(TweetSourceTask.class);
			input.getConfiguration().setString(TweetSourceTask.PROFILE, args[2]);
			input.setNumberOfSubtasks(1);
			input.setNumberOfSubtasksPerInstance(1);
	
			final JobTaskVertex hotTopicsTask = new JobTaskVertex("hot topics", jobGraph);
			hotTopicsTask.setTaskClass(HotTopicsRecognitionTask.class);
			hotTopicsTask.getConfiguration().setInteger(HotTopicsRecognitionTask.HISTORY_SIZE, 1000);
			hotTopicsTask.getConfiguration().setInteger(HotTopicsRecognitionTask.TOP_COUNT, 40);
			hotTopicsTask.setElasticNumberOfSubtasks(1, 10, 5);
			hotTopicsTask.setNumberOfSubtasksPerInstance(4);
	
			final JobTaskVertex topicsMergerTask = new JobTaskVertex("merger", jobGraph);
			topicsMergerTask.setTaskClass(HotTopicsMergerTask.class);
			topicsMergerTask.setNumberOfSubtasks(1);
			topicsMergerTask.setNumberOfSubtasksPerInstance(1);
	
			final JobTaskVertex filterTask = new JobTaskVertex("filter", jobGraph);
			filterTask.setTaskClass(FilterTask.class);
			filterTask.setElasticNumberOfSubtasks(1, 10, 1);
			filterTask.setNumberOfSubtasksPerInstance(4);
	
			final JobTaskVertex sentimentAnalysisTask = new JobTaskVertex("sentiment", jobGraph);
			sentimentAnalysisTask.setTaskClass(SentimentAnalysisTask.class);
			sentimentAnalysisTask.setElasticNumberOfSubtasks(1, 10, 1);
			sentimentAnalysisTask.setNumberOfSubtasksPerInstance(4);
	
	
			final JobOutputVertex output = new JobOutputVertex("Number Sink", jobGraph);
			output.setOutputClass(FileLineWriter.class);
			output.getConfiguration().setString("outputPath", "file://" + outputPath);
			output.setNumberOfSubtasks(1);
			output.setNumberOfSubtasksPerInstance(1);
			
			input.connectTo(hotTopicsTask, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			input.connectTo(filterTask, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			hotTopicsTask.connectTo(topicsMergerTask, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			topicsMergerTask.connectTo(filterTask, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			filterTask.connectTo(sentimentAnalysisTask, ChannelType.NETWORK,
					DistributionPattern.POINTWISE);
			sentimentAnalysisTask.connectTo(output, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);

			defineLatencyConstraintsTheComplicatedWay(jobGraph, input,
					hotTopicsTask, topicsMergerTask, filterTask,
					sentimentAnalysisTask, output);		

			// Create jar file for job deployment
			Process p = Runtime.getRuntime().exec("mvn clean package");	
			if (p.waitFor() != 0) {
				System.out.println("Failed to build twitter-sentiment-git.jar");
				System.exit(1);
			}
	
	
	//		jobGraph.addJar(new Path("target/twitter-sentiment-git.jar"));
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

	public static void defineLatencyConstraintsTheComplicatedWay(
			JobGraph jobGraph, final JobInputVertex input,
			final JobTaskVertex hotTopicsTask,
			final JobTaskVertex topicsMergerTask,
			final JobTaskVertex filterTask,
			final JobTaskVertex sentimentAnalysisTask,
			final JobOutputVertex output) throws JobGraphDefinitionException,
			IOException {

		JobGraphSequence seq1 = new JobGraphSequence();
		seq1.add(new SequenceElement<JobVertexID>(input.getID(), 1, filterTask
				.getID(), 0, 0, "edge0"));
		seq1.add(new SequenceElement<JobVertexID>(filterTask.getID(), 0, 0, 1,
				"filter"));
		seq1.add(new SequenceElement<JobVertexID>(filterTask.getID(), 0,
				sentimentAnalysisTask.getID(), 0, 2, "edge1"));
		seq1.add(new SequenceElement<JobVertexID>(
				sentimentAnalysisTask.getID(), 0, 0, 3, "sentiment"));
		seq1.add(new SequenceElement<JobVertexID>(
				sentimentAnalysisTask.getID(), 0, output.getID(), 0, 4, "edge2"));
		ConstraintUtil.addConstraint(new JobGraphLatencyConstraint(seq1, 30),
				jobGraph);

		JobGraphSequence seq2 = new JobGraphSequence();
		seq2.add(new SequenceElement<JobVertexID>(input.getID(), 0,
				hotTopicsTask.getID(), 0, 0, "edge0"));
		seq2.add(new SequenceElement<JobVertexID>(hotTopicsTask.getID(), 0, 0,
				1, "hot topics"));
		seq2.add(new SequenceElement<JobVertexID>(hotTopicsTask.getID(), 0,
				topicsMergerTask.getID(), 0, 2, "edge1"));
		seq2.add(new SequenceElement<JobVertexID>(topicsMergerTask.getID(), 0,
				0, 3, "merger"));
		seq2.add(new SequenceElement<JobVertexID>(topicsMergerTask.getID(), 0,
				filterTask.getID(), 1, 4, "edge2"));
		ConstraintUtil.addConstraint(new JobGraphLatencyConstraint(seq2, 210),
				jobGraph);

		// uncomment once constraint util bug is fixed
		// ConstraintUtil.defineAllLatencyConstraintsBetween(input.getForwardConnection(1),
		// sentimentAnalysisTask.getForwardConnection(0), 30);
		// ConstraintUtil.defineAllLatencyConstraintsBetween(
		// input.getForwardConnection(0),
		// topicsMergerTask.getForwardConnection(0), 210);
	}

	private static void printUsage() {
		System.err
				.println("Parameters: <jobmanager-host>:<port> <output-file> <profile>");
	}
}
