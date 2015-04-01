package de.tuberlin.cit.test.queuebehavior;

import org.apache.commons.cli.*;

import java.util.Arrays;

/**
 * Created by bjoern on 4/1/15.
 */
public class CliHelper {

	public static Options createOptions() {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("<minDop>:<initialDop>:<maxDop>")
						.hasArg()
						.withDescription("Specifies whether the job should be executed in elastic mode. " +
										"Format: <minDop>:<initialDop>:<maxDop>")
						.isRequired(false)
						.create("elastic"));

		options.addOption(OptionBuilder.withArgName("directory")
						.hasArg()
						.withDescription("Specifies a directory with jar files to attach to the JobGraph. Unless " +
										"specified, jar files from default locations (./target/ and ~/.m2/) will be attached.")
						.isRequired(false)
						.create("libdir"));

		options.addOption(OptionBuilder.withArgName("constraint")
						.hasArg()
						.withDescription("Latency constraint in milliseconds. Default is 20 ms.")
						.isRequired(false)
						.create("constraint"));

		options.addOption(OptionBuilder.withArgName("jobmanager-host:port")
						.hasArg()
						.withDescription("Jobmanager address.")
						.isRequired(true)
						.create("jmaddress"));

		options.addOption(OptionBuilder.withArgName("profileName")
						.hasArg()
						.withDescription("Profile for arallelism and load generation. Available profiles: "
										+ Arrays.toString(TestQueueBehaviorJobProfile.PROFILES.keySet().toArray()))
						.isRequired(true)
						.create("profile"));

		options.addOption(OptionBuilder.withArgName("file")
						.hasArg()
						.withDescription("Location of latency logfile written by each sink task.")
						.isRequired(true)
						.create("latencyLog"));

		return options;
	}

	public static void printUsage() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("test-queue-behavior", createOptions());
	}

	public static CommandLine parseArgs(String[] args) {
		try {
			CommandLineParser parser = new BasicParser();
			return parser.parse(createOptions(), args);
		} catch (ParseException exp) {
			System.err.println(exp.getMessage());
			printUsage();
			System.exit(1);
			return null;
		}
	}
}
