package de.tuberlin.cit.livescale.job.util.overlay;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;

import de.tuberlin.cit.livescale.job.record.VideoFrame;

public final class TimeVideoOverlay implements VideoOverlay {

	private final int X_OFFSET = 50;

	private final int Y_OFFSET = 10;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void draw(final VideoFrame frame) {

		final float time = (frame.timestampInNanos / 1000000000.0f);
		final BufferedImage image = frame.frameImage;

		final Graphics2D g = (Graphics2D) image.getGraphics();
		g.setColor(Color.WHITE);
		g.drawString(String.format("%.1f", time), image.getWidth() - this.X_OFFSET, this.Y_OFFSET);
	}

}
