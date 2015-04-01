package de.tuberlin.cit.test.queuebehavior;

import java.util.Arrays;
import java.util.Map;

import de.tuberlin.cit.test.queuebehavior.util.Util;
import org.json.simple.JSONValue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import de.tuberlin.cit.test.queuebehavior.task.LatencyLoggerBolt;
import de.tuberlin.cit.test.queuebehavior.task.NumberSpout;
import de.tuberlin.cit.test.queuebehavior.task.PrimeNumberTestBolt;

public class TestQueueBehaviorJobStorm {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			printUsage();
			System.exit(1);
			return;
		}

		String mode = args[0];
		if (!mode.equals("local") && args[0].split(":").length != 2) {
			System.err.printf("Unknown mode: %s\n", mode);
			printUsage();
			System.exit(1);
			return;
		}
		
		String nimbusHost = null;
		int nimbusThriftPort = -1;
		
		if (!mode.equals("local")) {
			nimbusHost = args[0].split(":")[0];
			nimbusThriftPort = Integer.parseInt(args[0].split(":")[1]);
		}

		TestQueueBehaviorJobProfile profile = TestQueueBehaviorJobProfile.PROFILES
				.get(args[1]);
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[1]);
			printUsage();
			System.exit(1);
			return;
		}

		// Create jar file for job deployment
		Process p = Runtime.getRuntime().exec("mvn clean package");
		if (p.waitFor() != 0) {
			System.out.println("Failed to build test-queue-behavior.jar");
			System.exit(1);
		}

		long globalBeginTime = Util.alignToInterval(System.currentTimeMillis(), 1000) + 30000;

		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("numbers", new NumberSpout(args[1], globalBeginTime),
				profile.paraProfile.outerTaskDop);

		builder.setBolt("testedNumbers", new PrimeNumberTestBolt(),
				profile.paraProfile.innerTaskDop).shuffleGrouping("numbers");
		
		builder.setBolt("testedNumberSink", new LatencyLoggerBolt(args[2]),
				profile.paraProfile.outerTaskDop).shuffleGrouping(
				"testedNumbers");

		if (!mode.equals("local")) {
			@SuppressWarnings("unchecked")
			Map<String, Object> conf = (Map<String, Object>) Utils.readDefaultConfig();
			conf.put(Config.NIMBUS_HOST, nimbusHost);
			conf.put(Config.NIMBUS_THRIFT_PORT, nimbusThriftPort);
			conf.put(Config.TOPOLOGY_WORKERS, profile.paraProfile.outerTaskDop
					/ profile.paraProfile.outerTaskDopPerInstance);
			
			 // upload topology jar to Cluster using StormSubmitter
			String uploadedJarLocation = StormSubmitter.submitJar(conf, "target/test-queue-behavior-git.jar");
 
			NimbusClient nimbus = NimbusClient.getConfiguredClient(conf);
			String jsonConf = JSONValue.toJSONString(conf);
			nimbus.getClient().submitTopology("TestQueueBehavior",
					uploadedJarLocation, jsonConf, builder.createTopology());			
			
		} else if (mode.equals("local")) {
			Config conf = new Config();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("TestQueueBehavior", conf,
					builder.createTopology());
			Utils.sleep(60000 + profile.loadGenProfile.getTotalDuration());
			cluster.killTopology("TestQueueBehavior");
			cluster.shutdown();
		}
	}
	
	private static void printUsage() {
		System.err.println("Parameters: <mode> <profile-name> <latency-logfile>");
		System.err.println("Available modes: local, nimbushost:thriftport");
		System.err.printf("Available profiles: %s\n",
				Arrays.toString(TestQueueBehaviorJobProfile.PROFILES.keySet().toArray()));
	}	
}
