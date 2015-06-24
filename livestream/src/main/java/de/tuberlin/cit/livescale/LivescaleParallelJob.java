package de.tuberlin.cit.livescale;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

import de.tuberlin.cit.livescale.job.CliHelper;
import de.tuberlin.cit.livescale.job.task.BroadcasterTask;
import de.tuberlin.cit.livescale.job.task.DecoderTask;
import de.tuberlin.cit.livescale.job.task.EncoderTask;
import de.tuberlin.cit.livescale.job.task.MergeTask;
import de.tuberlin.cit.livescale.job.task.OverlayTask;
import de.tuberlin.cit.livescale.job.task.VideoFileStreamSourceTask;
import de.tuberlin.cit.livescale.job.util.encoder.VideoEncoder;
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
import org.apache.commons.cli.CommandLine;

/**
 * This Nephele job is intended for non-interactive cluster experiments with the
 * Livescale toolkit. It is mainly intended to test the Stratosphere Streaming
 * Distribution and the Livescale toolkit.
 * 
 * This Nephele job reads "packetized" video files from local disks inside the
 * cluster decodes the video, manipulates it, reencodes it and makes it
 * available as mpegts over http.
 * 
 * @author Bjoern Lohrmann
 */
public class LivescaleParallelJob {

	public static void main(final String[] args) {

		CommandLine cli = CliHelper.parseArgs(args);

		String jmHost = cli.getOptionValue("jmaddress").split(":")[0];
		int jmPort = Integer.parseInt(cli.getOptionValue("jmaddress").split(":")[1]);
		
		LivescaleParallelJobProfile profile = new LivescaleParallelJobProfile("custom",
						Integer.parseInt(cli.getOptionValue("innerDop")), 4,
						Integer.parseInt(cli.getOptionValue("outerDop")), 1,
						Integer.parseInt(cli.getOptionValue("streams")),
						Integer.parseInt(cli.getOptionValue("groupSize")));
		
		String videoDir = cli.getOptionValue("videoDir");
		
		try {
			final JobGraph graph = new JobGraph("Streaming Job with File Input");

			final JobInputVertex fileStreamSource = new JobInputVertex(
					"MultiFileStreamSource", graph);
			fileStreamSource.setInputClass(VideoFileStreamSourceTask.class);
			fileStreamSource.setNumberOfSubtasks(profile.outerTaskDop);
			fileStreamSource.setNumberOfSubtasksPerInstance(profile.outerTaskDopPerInstance);
			fileStreamSource.getConfiguration().setInteger(
					VideoFileStreamSourceTask.NO_OF_STREAMS_PER_SUBTASK,
					Math.max(1, profile.noOfStreams / profile.outerTaskDop));
			fileStreamSource.getConfiguration().setInteger(
					VideoFileStreamSourceTask.NO_OF_STREAMS_PER_GROUP,
					profile.noOfStreamsPerGroup);
			fileStreamSource.getConfiguration().setString(
					VideoFileStreamSourceTask.VIDEO_FILE_DIRECTORY, videoDir);
			

			final JobTaskVertex decoder = new JobTaskVertex("Decoder", graph);
			decoder.setTaskClass(DecoderTask.class);
			configureInnerTaskParallelism(decoder, profile);
			
			final JobTaskVertex merger = new JobTaskVertex("Merger", graph);
			merger.setTaskClass(MergeTask.class);
			configureInnerTaskParallelism(merger, profile);
			
			final JobTaskVertex overlay = new JobTaskVertex("Overlay", graph);
			overlay.setTaskClass(OverlayTask.class);
			configureInnerTaskParallelism(overlay, profile);

			final JobTaskVertex encoder = new JobTaskVertex("Encoder", graph);
			encoder.setTaskClass(EncoderTask.class);
			encoder.getConfiguration().setString(
					VideoEncoder.ENCODER_OUTPUT_FORMAT, "mpegts");
			configureInnerTaskParallelism(encoder, profile);

			final JobOutputVertex output = new JobOutputVertex("Receiver",
					graph);
			output.setOutputClass(BroadcasterTask.class);
			output.setNumberOfSubtasks(profile.outerTaskDop);
			output.setNumberOfSubtasksPerInstance(1);
			output.getConfiguration().setString(BroadcasterTask.BROADCAST_TRANSPORT, "http");
			output.getConfiguration().setString(BroadcasterTask.LATENCY_LOG, cli.getOptionValue("latencyLog"));

			fileStreamSource.connectTo(decoder, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);
			decoder.connectTo(merger, ChannelType.NETWORK,
					DistributionPattern.POINTWISE);
			merger.connectTo(overlay, ChannelType.NETWORK,
					DistributionPattern.POINTWISE);
			overlay.connectTo(encoder, ChannelType.NETWORK,
					DistributionPattern.POINTWISE);
			encoder.connectTo(output, ChannelType.NETWORK,
					DistributionPattern.BIPARTITE);

			ConstraintUtil.defineAllLatencyConstraintsBetween(
					fileStreamSource.getForwardConnection(0),
					encoder.getForwardConnection(0), Integer.parseInt(cli.getOptionValue("constraint")));

			// Configure instance sharing
			decoder.setVertexToShareInstancesWith(fileStreamSource);
			merger.setVertexToShareInstancesWith(fileStreamSource);
			overlay.setVertexToShareInstancesWith(fileStreamSource);
			encoder.setVertexToShareInstancesWith(fileStreamSource);
			output.setVertexToShareInstancesWith(fileStreamSource);

			addJars(cli, graph);

			Configuration conf = new Configuration();
			conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
					jmHost);
			conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jmPort);

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

	private static void addDependencyJars(final JobGraph graph) {
		String uHome = System.getProperty("user.home") + "/.m2/repository/";
		graph.addJar(new Path(uHome + "/com/rabbitmq/amqp-client/2.8.4/amqp-client-2.8.4.jar"));
		graph.addJar(new Path(uHome + "/de/tuberlin/cit/livescale-messaging/git/livescale-messaging-git.jar"));
		graph.addJar(new Path(uHome + "/xuggle/xuggle-xuggler/5.4/xuggle-xuggler-5.4.jar"));
		graph.addJar(new Path(uHome + "/org/slf4j/slf4j-api/1.6.4/slf4j-api-1.6.4.jar"));
		graph.addJar(new Path(uHome + "/commons-cli/commons-cli/1.1/commons-cli-1.1.jar"));
		graph.addJar(new Path(uHome + "/ch/qos/logback/logback-classic/1.0.0/logback-classic-1.0.0.jar"));
		graph.addJar(new Path(uHome + "/ch/qos/logback/logback-core/1.0.0/logback-core-1.0.0.jar"));
	}

	private static void compileMaven() throws IOException, InterruptedException {
		// Create jar file for job deployment
		Process p = Runtime.getRuntime().exec("mvn clean package");
		if (p.waitFor() != 0) {
			System.out.println("Failed to build livestream-git.jar");
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
			graph.addJar(new Path("target/livestream-git.jar"));
			addDependencyJars(graph);
		}
	}


	private static void configureInnerTaskParallelism(JobTaskVertex innerTask,
			LivescaleParallelJobProfile profile) {
		innerTask.setNumberOfSubtasks(profile.innerTaskDop);
		innerTask.setNumberOfSubtasksPerInstance(profile.innerTaskDopPerInstance);
	}
}
