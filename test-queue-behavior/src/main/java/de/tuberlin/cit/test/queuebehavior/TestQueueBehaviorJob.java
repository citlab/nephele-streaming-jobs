package de.tuberlin.cit.test.queuebehavior;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import de.tuberlin.cit.test.queuebehavior.task.NumberSourceTask;
import de.tuberlin.cit.test.queuebehavior.task.PrimeNumberTestTask;
import de.tuberlin.cit.test.queuebehavior.task.LatencyLoggerTask;
import de.tuberlin.cit.test.queuebehavior.util.Util;
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
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import org.apache.commons.cli.*;

/**
 * This Nephele job is intended for non-interactive cluster experiments with
 * Nephele and designed to observe the effects of queueing.
 * <p/>
 * This Nephele job generates large numbers (which is a fairly cheap operation)
 * at an increasing rate and tests them for primeness (which is compute
 * intensive).
 *
 * @author Bjoern Lohrmann
 */
public class TestQueueBehaviorJob {

	public static void main(final String[] args) {

		CommandLine cli = CliHelper.parseArgs(args);

		String jmHost = cli.getOptionValue("jmaddress").split(":")[0];
		int jmPort = Integer.parseInt(cli.getOptionValue("jmaddress").split(":")[1]);

		TestQueueBehaviorJobProfile profile = TestQueueBehaviorJobProfile.PROFILES.get(cli.getOptionValue("profile"));
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", cli.getOptionValue("profile"));
			CliHelper.printUsage();
			System.exit(1);
			return;
		}

		String latencyLogfile = cli.getOptionValue("latencyLog");

		try {
			final JobGraph graph = new JobGraph("Test Queue Behavior job");

			final JobInputVertex numberSource = new JobInputVertex("Number Source", graph);
			numberSource.setInputClass(NumberSourceTask.class);
			numberSource.getConfiguration().setString(NumberSourceTask.PROFILE_PROPERTY_KEY, profile.name);
			numberSource.getConfiguration().setLong(NumberSourceTask.GLOBAL_BEGIN_TIME_PROPERTY_KEY,
							Util.alignToInterval(System.currentTimeMillis(), 1000) + 30000);
			numberSource.setNumberOfSubtasks(32);
			numberSource.setNumberOfSubtasksPerInstance(4);
//			numberSource.setNumberOfSubtasks(profile.paraProfile.outerTaskDop);
//			numberSource.setNumberOfSubtasksPerInstance(profile.paraProfile.outerTaskDopPerInstance);

			final JobTaskVertex primeTester = new JobTaskVertex(
							"Prime Tester", graph);
			primeTester.setTaskClass(PrimeNumberTestTask.class);
			primeTester.setNumberOfSubtasksPerInstance(profile.paraProfile.innerTaskDopPerInstance);

			if (cli.hasOption("elastic")) {
				int min = Integer.parseInt(cli.getOptionValue("elastic").split(":")[0]);
				int initial = Integer.parseInt(cli.getOptionValue("elastic").split(":")[1]);
				int max = Integer.parseInt(cli.getOptionValue("elastic").split(":")[2]);
				primeTester.setElasticNumberOfSubtasks(min, max, initial);
			} else if (cli.hasOption("unelastic")) {
				primeTester.setNumberOfSubtasks(Integer.parseInt(cli.getOptionValue("unelastic")));
			} else {
				primeTester.setNumberOfSubtasks(profile.paraProfile.innerTaskDop);
			}

			final JobOutputVertex numberSink = new JobOutputVertex("Number Sink", graph);
			numberSink.setOutputClass(LatencyLoggerTask.class);
			numberSink.setNumberOfSubtasksPerInstance(profile.paraProfile.outerTaskDopPerInstance);
			if (cli.hasOption("elastic")) {
				int primeTesterMax = Integer.parseInt(cli.getOptionValue("elastic").split(":")[2]);
				numberSink.setNumberOfSubtasks(primeTesterMax / primeTester.getNumberOfSubtasksPerInstance());
			} else {
				numberSink.setNumberOfSubtasks(profile.paraProfile.outerTaskDop);
			}
			numberSink.getConfiguration().setString(LatencyLoggerTask.LATENCY_LOG_KEY, latencyLogfile);

			numberSource.connectTo(primeTester, ChannelType.NETWORK,
							DistributionPattern.BIPARTITE);
			primeTester.connectTo(numberSink, ChannelType.NETWORK,
							DistributionPattern.BIPARTITE);

			int constraintMs = cli.hasOption("constraint")
							? Integer.parseInt(cli.getOptionValue("constraint"))
							: 20;
			ConstraintUtil.defineAllLatencyConstraintsBetween(
							numberSource.getForwardConnection(0),
							primeTester.getForwardConnection(0), constraintMs);

			// Configure instance sharing when executing locally
//			primeTester.setVertexToShareInstancesWith(numberSource);
			numberSink.setVertexToShareInstancesWith(primeTester);

			addJars(cli, graph);

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

	private static void compileMaven() throws IOException, InterruptedException {
		// Create jar file for job deployment
		Process p = Runtime.getRuntime().exec("mvn clean package");
		if (p.waitFor() != 0) {
			System.out.println("Failed to build test-queue-behavior.jar");
			System.exit(1);
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

			String uHome = System.getProperty("user.home");
			graph.addJar(new Path("target/test-queue-behavior-git.jar"));
			graph.addJar(new Path(uHome + "/.m2/repository/org/slf4j/slf4j-log4j12/1.6.5/slf4j-log4j12-1.6.5.jar"));
			graph.addJar(new Path(uHome + "/.m2/repository/org/slf4j/slf4j-api/1.6.5/slf4j-api-1.6.5.jar"));
			graph.addJar(new Path(uHome + "/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"));
		}
	}
}
