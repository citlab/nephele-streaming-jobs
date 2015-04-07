package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import org.apache.commons.cli.*;

import java.util.Arrays;

/**
 * Created by bjoern on 4/1/15.
 */
public class CliHelper {

	public static Options createOptions() {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("jobmanager-host:port")
						.hasArg()
						.withDescription("Jobmanager address.")
						.isRequired(true)
						.create("jmaddress"));

		options.addOption(OptionBuilder.withArgName("profileName")
						.hasArg()
						.withDescription("Profile for other task settings. Available profiles: "
										+ Arrays.toString(TwitterSentimentJobProfile.PROFILES.keySet().toArray()))
						.isRequired(true)
						.create("profile"));

		options.addOption(OptionBuilder.withArgName("<minDop>:<initialDop>:<maxDop>")
						.hasArg()
						.withDescription("Specifies elastic degree of parallelism for HotTopicsRegcognition task" +
										"Format: <minDop>:<initialDop>:<maxDop>")
						.isRequired(true)
						.create("htDop"));

		options.addOption(OptionBuilder.withArgName("<minDop>:<initialDop>:<maxDop>")
						.hasArg()
						.withDescription("Specifies elastic degree of parallelism for Filter task" +
										"Format: <minDop>:<initialDop>:<maxDop>")
						.isRequired(true)
						.create("filterDop"));

		options.addOption(OptionBuilder.withArgName("<minDop>:<initialDop>:<maxDop>")
						.hasArg()
						.withDescription("Specifies elastic degree of parallelism for Sentiment task" +
										"Format: <minDop>:<initialDop>:<maxDop>")
						.isRequired(true)
						.create("sentimentDop"));

		options.addOption(OptionBuilder.withArgName("constraint1")
						.hasArg()
						.withDescription("Latency constraint of Src-Filter-Sentiment-Snk in milliseconds. Default is 15 ms.")
						.isRequired(false)
						.create("constraint1"));

		options.addOption(OptionBuilder.withArgName("constraint2")
						.hasArg()
						.withDescription("Latency constraint of Src-HotTopicsRecognizer-HotTopicsMerger-Filter in milliseconds. Default is 30 ms.")
						.isRequired(false)
						.create("constraint2"));

		options.addOption(OptionBuilder.withArgName("file")
						.hasArg()
						.withDescription("Location of latency logfile written by sink task (in-band measurement of constraint 1 latency).")
						.isRequired(true)
						.create("latencyLogSink"));

		options.addOption(OptionBuilder.withArgName("file")
						.hasArg()
						.withDescription("Location of latency logfile written by filter task (in-band measurement of constraint 2 latency).")
						.isRequired(true)
						.create("latencyLogFilter"));

		options.addOption(OptionBuilder.withArgName("file")
						.hasArg()
						.withDescription("Location of tweet logfile (with sentiments) written by sink task.")
						.isRequired(true)
						.create("tweetLog"));


		options.addOption(OptionBuilder.withArgName("file")
						.hasArg()
						.withDescription("Slowdown factor of synthetic benchmark time (if < 1, then it is speeds up synthetic time). Default is 0.005")
						.isRequired(false)
						.create("factor"));

		options.addOption(OptionBuilder.withArgName("directory")
						.hasArg()
						.withDescription("Specifies a directory with jar files to attach to the JobGraph. Unless " +
										"specified, a jar file from default (./target/) will be attached.")
						.isRequired(false)
						.create("libdir"));

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
