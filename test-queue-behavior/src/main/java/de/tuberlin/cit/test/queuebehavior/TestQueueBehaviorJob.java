package de.tuberlin.cit.test.queuebehavior;

import java.io.IOException;

import de.tuberlin.cit.test.queuebehavior.task.NumberSourceTask;
import de.tuberlin.cit.test.queuebehavior.task.PrimeNumberTestTask;
import de.tuberlin.cit.test.queuebehavior.task.ReceiverTask;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;

/**
 * This Nephele job is intended for non-interactive cluster experiments with
 * Nephele and designed to observe the effects of queueing.
 * 
 * This Nephele job generates large numbers (which is a fairly cheap operation)
 * at an increasing rate and tests them for primeness (which is compute
 * intensive).
 * 
 * @author Bjoern Lohrmann
 */
public class TestQueueBehaviorJob {

	private static final String INSTANCE_TYPE = "default";

	private static final int NUMBER_OF_SUBTASKS_PER_INSTANCE = 1;

	public static void main(final String[] args) {

		if (args.length != 1) {
			System.err.println("Parameters: <degree of parallelism>");
			System.exit(1);
			return;
		}

		int degreeOfParallelism;
		try {
			degreeOfParallelism = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.exit(1);
			return;
		}

		if (degreeOfParallelism < 1) {
			System.err.println("Degree of parallelism must be greater than 0");
			System.exit(1);
			return;
		}

		try {
			final JobGraph graph = new JobGraph("Test Queue Behavior job");

			final int numberOfSourceTasks = Math
					.max(1, degreeOfParallelism / 8);

			final JobInputVertex numberSource = new JobInputVertex(
					"Number Source", graph);
			numberSource.setInputClass(NumberSourceTask.class);
			numberSource.setNumberOfSubtasks(numberOfSourceTasks);
			numberSource.setInstanceType(INSTANCE_TYPE);
			numberSource.setNumberOfSubtasksPerInstance(1);

			final JobTaskVertex primeTester = new JobTaskVertex(
					"Prime Tester", graph);
			primeTester.setNumberOfSubtasks(degreeOfParallelism);
			primeTester.setTaskClass(PrimeNumberTestTask.class);
			primeTester.setInstanceType(INSTANCE_TYPE);
			primeTester.setNumberOfSubtasksPerInstance(NUMBER_OF_SUBTASKS_PER_INSTANCE);

			final JobOutputVertex numberSink = new JobOutputVertex("Number Sink",
					graph);
			numberSink.setOutputClass(ReceiverTask.class);
			numberSink.setInstanceType(INSTANCE_TYPE);
			numberSink.setNumberOfSubtasksPerInstance(NUMBER_OF_SUBTASKS_PER_INSTANCE);

			numberSource.connectTo(primeTester, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			primeTester.connectTo(numberSink, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);

			// ConstraintUtil.defineAllLatencyConstraintsBetween(
			// fileStreamSource.getForwardConnection(0),
			// decoder.getForwardConnection(0), 100);

			// Configure instance sharing when executing locally
			primeTester.setVertexToShareInstancesWith(numberSource);
			numberSink.setVertexToShareInstancesWith(numberSource);

			// Create jar file for job deployment
			Process p = Runtime.getRuntime().exec("mvn clean package");
			if (p.waitFor() != 0) {
				System.out.println("Failed to build test-queue-behavior.jar");
				System.exit(1);
			}

			graph.addJar(new Path("target/test-queue-behavior-git.jar"));

			Configuration conf = new Configuration();
			conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
					"127.0.0.1");
			conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
					ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

			final JobClient jobClient = new JobClient(graph, conf);
			jobClient.submitJobAndWait();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (JobGraphDefinitionException e) {
			e.printStackTrace();
		} catch (JobExecutionException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
		}
	}
}
