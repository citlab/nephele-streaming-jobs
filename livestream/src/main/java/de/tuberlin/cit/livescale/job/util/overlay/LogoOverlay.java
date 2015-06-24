/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.cit.livescale.job.util.overlay;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;

import de.tuberlin.cit.livescale.job.record.VideoFrame;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class LogoOverlay implements VideoOverlay {

	private final BufferedImage originalLogoImage;

	private Image scaledLogo;

	private int scaledWidth;

	private int scaledHeight;

	/**
	 * Initializes LogoOverlay.
	 * 
	 * @param string
	 */
	public LogoOverlay(BufferedImage logoImage) {
		this.originalLogoImage = logoImage;
		this.scaledLogo = this.originalLogoImage;
		this.scaledWidth = this.originalLogoImage.getWidth();
		this.scaledHeight = this.originalLogoImage.getHeight();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * de.tuberlin.cit.livescale.job.util.overlay.VideoOverlay#draw(de.tuberlin
	 * .cit.livescale.job.record.VideoFrame)
	 */
	@Override
	public void draw(VideoFrame frame) {

		BufferedImage frameImage = frame.frameImage;

		int maxWidth = (int) (0.25 * frameImage.getWidth());
		int maxHeight = (int) (0.25 * frameImage.getHeight());

		ensureScaledLogoDimensions(maxWidth, maxHeight);
		drawScaledLogo(frameImage);
	}

	/**
	 * @param maxWidth
	 * @param maxHeight
	 */
	private void ensureScaledLogoDimensions(int maxWidth, int maxHeight) {
		if (this.scaledWidth > maxWidth || this.scaledHeight > maxHeight) {

			double scaleFactor = Math.min(
					maxWidth / ((double) this.originalLogoImage.getWidth()), maxHeight
							/ ((double) this.originalLogoImage.getHeight()));

			this.scaledWidth = (int) (scaleFactor * this.originalLogoImage.getWidth());
			this.scaledHeight = (int) (scaleFactor * this.originalLogoImage.getHeight());
			this.scaledLogo = this.originalLogoImage.getScaledInstance(this.scaledWidth,
					this.scaledHeight, Image.SCALE_SMOOTH);
		}
	}

	/**
	 * @param frameImage
	 */
	private void drawScaledLogo(BufferedImage frameImage) {
		int x = 15;
//		int y = frameImage.getHeight() - scaledHeight - 15;
		int y = 15;
		Graphics2D g = (Graphics2D) frameImage.getGraphics();
		g.drawImage(this.scaledLogo, x, y, null);
	}
}
