package de.tuberlin.cit.livescale.job.util.merge;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

import de.tuberlin.cit.livescale.job.record.VideoFrame;

public final class MergeGroup {

	private static final int QUEUE_LENGTH_LIMIT = 100;

	private static final int BORDER = 4;

	private final Map<Long, Queue<VideoFrame>> frameQueues = new LinkedHashMap<Long, Queue<VideoFrame>>();

	public void addFrame(final VideoFrame frame) {

		Queue<VideoFrame> queue = this.frameQueues.get(Long.valueOf(frame.streamId));
		if (queue == null) {
			queue = new ArrayDeque<VideoFrame>();
			this.frameQueues.put(Long.valueOf(frame.streamId), queue);
		}

		if (queue.size() < QUEUE_LENGTH_LIMIT) {
			queue.add(frame);
		} else {
			System.out.println("Dropping frame");
		}
	}

	public VideoFrame mergedFrameAvailable() {

		if (!allQueuesHaveFramesAvailable()) {
			return null;
		}

		final VideoFrame[] videoFrames = new VideoFrame[this.frameQueues.size()];
		final Iterator<Queue<VideoFrame>> it = this.frameQueues.values().iterator();
		int count = 0;
		while (it.hasNext()) {
			videoFrames[count++] = it.next().poll();
		}

		return mergeFrames(videoFrames);
	}

	private boolean allQueuesHaveFramesAvailable() {

		final Iterator<Queue<VideoFrame>> it = this.frameQueues.values().iterator();
		while (it.hasNext()) {
			if (it.next().isEmpty()) {
				return false;
			}
		}

		return true;
	}

	private VideoFrame mergeFrames(final VideoFrame[] videoFrames) {

		final BufferedImage sourceImage = videoFrames[0].frameImage;

		final int width = sourceImage.getWidth();
		final int height = sourceImage.getHeight();

		BufferedImage mergedFrame = new BufferedImage(width, height, sourceImage.getType());

		Graphics2D g = (Graphics2D) mergedFrame.getGraphics();
		g.setColor(Color.DARK_GRAY);
		g.fillRect(0, 0, width, height);

		final int sqrt = (int) Math.ceil(Math.sqrt(videoFrames.length));
		for (int i = 0; i < videoFrames.length; ++i) {

			final int imageWidth = (width - (sqrt + 1) * BORDER) / sqrt;
			final int imageHeight = (height - (sqrt + 1) * BORDER) / sqrt;

			final int colIndex = (i % sqrt);
			final int rowIndex = (i / sqrt);

			final int imageX = ((BORDER + imageWidth) * colIndex) + BORDER;
			final int imageY = ((BORDER + imageHeight) * rowIndex) + BORDER;

			g.drawImage(videoFrames[i].frameImage, imageX, imageY, imageX + imageWidth, imageY + imageHeight, 0, 0,
				width, height, null);

		}

		videoFrames[0].frameImage = mergedFrame;

		return videoFrames[0];
	}
}
