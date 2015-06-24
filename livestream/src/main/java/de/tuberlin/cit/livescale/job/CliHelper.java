package de.tuberlin.cit.livescale.job;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Arrays;

/**
 * Created by bjoern on 4/1/15.
 */
public class CliHelper {

	public static Options createOptions() {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("<innerDop>")
						.hasArg()
						.withDescription("Degree of parallelism of inner tasks (Decoder, Merger, etc)")
						.isRequired(true)
						.create("innerDop"));

		options.addOption(OptionBuilder.withArgName("<outerDop>")
						.hasArg()
						.withDescription("Degree of parallelism of outer tasks (Source, Sink)")
						.isRequired(true)
						.create("outerDop"));

		options.addOption(OptionBuilder.withArgName("<directory>")
						.hasArg()
						.withDescription("Specifies a directory with jar files to attach to the JobGraph. Unless " +
										"specified, jar files from default locations (./target/ and ~/.m2/) will be attached.")
						.isRequired(false)
						.create("libdir"));

		options.addOption(OptionBuilder.withArgName("<constraint>")
						.hasArg()
						.withDescription("Latency constraint in milliseconds.")
						.isRequired(true)
						.create("constraint"));

		options.addOption(OptionBuilder.withArgName("<jobmanager-host:port>")
						.hasArg()
						.withDescription("Jobmanager address.")
						.isRequired(true)
						.create("jmaddress"));

		options.addOption(OptionBuilder.withArgName("<streams>")
						.hasArg()
						.withDescription("Number of video streams to create.")
						.isRequired(true)
						.create("streams"));

		options.addOption(OptionBuilder.withArgName("<groupSize>")
						.hasArg()
						.withDescription("The size of groups that videos shall be merged into.")
						.isRequired(true)
						.create("groupSize"));

		options.addOption(OptionBuilder.withArgName("<file>")
						.hasArg()
						.withDescription("Location of latency logfile written by each sink task.")
						.isRequired(true)
						.create("latencyLog"));

		options.addOption(OptionBuilder.withArgName("<video-dir>")
						.hasArg()
						.withDescription("Directory with video files to stream.")
						.isRequired(true)
						.create("videoDir"));

		return options;
	}

	public static void printUsage() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("livestream", createOptions());
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
