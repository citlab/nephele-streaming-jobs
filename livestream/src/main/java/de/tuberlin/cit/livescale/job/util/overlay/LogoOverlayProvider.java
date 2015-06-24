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

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.imageio.ImageIO;

import eu.stratosphere.nephele.configuration.Configuration;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class LogoOverlayProvider implements OverlayProvider {

	public static final String LOGO_OVERLAY_IMAGE = "overlay.logo.image";

	private final BufferedImage logoImage;

	private final HashMap<Long, LogoOverlay> overlaysByStream = new HashMap<>();

	public LogoOverlayProvider(Configuration conf) throws IOException {
		this.logoImage = ImageIO.read(new File(conf.getString(
				LOGO_OVERLAY_IMAGE, "logo.png")));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#start()
	 */
	@Override
	public void start() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#stop()
	 */
	@Override
	public void stop() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#
	 * dropOverlayForStream(long)
	 */
	@Override
	public void dropOverlayForStream(long streamId) {
		this.overlaysByStream.remove(streamId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#
	 * getOverlayForStream(long)
	 */
	@Override
	public VideoOverlay getOverlayForStream(long streamId) {
		LogoOverlay ret = this.overlaysByStream.get(streamId);
		if (ret == null) {
			ret = new LogoOverlay(this.logoImage);
			this.overlaysByStream.put(streamId, ret);
		}
		return ret;
	}
}
