package de.tuberlin.cit.test.queuebehavior.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyLog {
	public static final int LOG_INTERVAL_MILLIS = 10000;

	private static final Logger LOG = LoggerFactory.getLogger(LatencyLog.class);

	private ExecutorService backgroundWorker = Executors.newSingleThreadExecutor();

	private Writer out;

	private long aggregatedLatencies;

	private int counter;

	private long minLatency;

	private long maxLatency;

	private long nextLogIntervalBegin;

	private ArrayList<Long> latencies;

	private double sampleProb = 0.1;

	public LatencyLog(String file) throws IOException {
		out = new FileWriter(file);
		out.write("timestamp;avgLatency;minLatency;maxLatency;perc25;perc50;perc75;perc80;perc90;perc95;perc99\n");
		out.flush();

		nextLogIntervalBegin = alignToInterval(System.currentTimeMillis() + LOG_INTERVAL_MILLIS,
						LOG_INTERVAL_MILLIS);

		reset();
	}

	private void reset() {
		aggregatedLatencies = 0;
		counter = 0;
		minLatency = Long.MAX_VALUE;
		maxLatency = Long.MIN_VALUE;
		latencies = new ArrayList<>();
	}

	public void log(long timestamp) {
		long now = System.currentTimeMillis();

		long latency = Math.max(0, now - timestamp);

		aggregatedLatencies += latency;
		counter++;
		minLatency = Math.min(minLatency, latency);
		maxLatency = Math.max(maxLatency, latency);

		if (ThreadLocalRandom.current().nextDouble() < sampleProb) {
			latencies.add(latency);
		}

		if (now >= nextLogIntervalBegin) {
			final String logLine = String.format("%d;%.1f;%d;%d",
							nextLogIntervalBegin / 2000,
							((double) aggregatedLatencies) / counter,
							minLatency, maxLatency);
//			sampleProb = Math.max(0.01, Math.min(1, (1000.0 / latencies.size()) * sampleProb));
			backgroundWorker.submit(createLogWorker(logLine, latencies, out));
			reset();

			while (nextLogIntervalBegin <= now) {
				nextLogIntervalBegin += LOG_INTERVAL_MILLIS;
			}
		}
	}

	public void close() {
		if (out != null) {
			backgroundWorker.submit(createCloseLogWorker(out));
			backgroundWorker.shutdown();
			out = null;
		}
	}

	private static long alignToInterval(long timestampInMillis, long interval) {
		long remainder = timestampInMillis % interval;

		return timestampInMillis - remainder;
	}

	private static Runnable createLogWorker(final String logLine, final ArrayList<Long> latencies, final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					Collections.sort(latencies);

					long percentile25 = getQuantileFromSortedList(latencies, 0.25);
					long percentile50 = getQuantileFromSortedList(latencies, 0.5);
					long percentile75 = getQuantileFromSortedList(latencies, 0.75);
					long percentile80 = getQuantileFromSortedList(latencies, 0.8);
					long percentile90 = getQuantileFromSortedList(latencies, 0.9);
					long percentile95 = getQuantileFromSortedList(latencies, 0.95);
					long percentile99 = getQuantileFromSortedList(latencies, 0.99);

					String actualLogLine = logLine + String.format("%d;%d;%d;%d;%d;%d;%d\n",
									percentile25,
									percentile50,
									percentile75,
									percentile80,
									percentile90,
									percentile95,
									percentile99);
					out.write(actualLogLine);
					out.flush();
				} catch (IOException e) {
					LOG.error("Error when writing to receiver latency log", e);
				}
			}

			private long getQuantileFromSortedList(ArrayList<Long> latencies, double quantile) {
				if (latencies.isEmpty()) {
					return 0;
				} else {
					return latencies.get((int) Math.floor(quantile * latencies.size()));
				}
			}
		};
	}

	private static Runnable createCloseLogWorker(final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					out.close();
				} catch (IOException e) {
					LOG.error("Error when closing receiver latency log", e);
				}
			}
		};

	}
}
