package de.tuberlin.cit.test.queuebehavior;

import java.io.IOException;
import java.util.Arrays;

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

	public static void main(final String[] args) {

		if (args.length != 2) {
			printUsage();
			System.exit(1);
			return;
		}
				
		String jmHost = args[0].split(":")[0];
		int jmPort = Integer.parseInt(args[0].split(":")[1]);
		
		TestQueueBehaviorJobProfile profile = TestQueueBehaviorJobProfile.PROFILES.get(args[1]);
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[1]);
			printUsage();
			System.exit(1);
			return;			
		}

		try {
			final JobGraph graph = new JobGraph("Test Queue Behavior job");

			final JobInputVertex numberSource = new JobInputVertex("Number Source", graph);
			numberSource.setInputClass(NumberSourceTask.class);
			numberSource.setNumberOfSubtasks(profile.outerTaskDop);
			numberSource.setNumberOfSubtasksPerInstance(profile.outerTaskDopPerInstance);

			final JobTaskVertex primeTester = new JobTaskVertex(
					"Prime Tester", graph);
			primeTester.setTaskClass(PrimeNumberTestTask.class);
			primeTester.setElasticNumberOfSubtasks(1, profile.innerTaskDop, 1);
			primeTester.setNumberOfSubtasksPerInstance(profile.innerTaskDopPerInstance);

			final JobOutputVertex numberSink = new JobOutputVertex("Number Sink", graph);
			numberSink.setOutputClass(ReceiverTask.class);
			numberSink.setNumberOfSubtasks(profile.outerTaskDop);
			numberSink.setNumberOfSubtasksPerInstance(profile.outerTaskDopPerInstance);


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
					jmHost);
			conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
					jmPort);

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

	private static void printUsage() {
		System.err.println("Parameters: <jobmanager-host>:<port> <profile-name>");
		System.err.printf("Available profiles: %s\n",
				Arrays.toString(TestQueueBehaviorJobProfile.PROFILES.keySet().toArray()));
	}	
}
