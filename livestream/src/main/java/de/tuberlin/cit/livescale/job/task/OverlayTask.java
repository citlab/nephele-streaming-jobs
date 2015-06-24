package de.tuberlin.cit.livescale.job.task;

import de.tuberlin.cit.livescale.job.record.VideoFrame;
import de.tuberlin.cit.livescale.job.util.overlay.LogoOverlayProvider;
import de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider;
import de.tuberlin.cit.livescale.job.util.overlay.TimeOverlayProvider;
import de.tuberlin.cit.livescale.job.util.overlay.VideoOverlay;
import eu.stratosphere.nephele.template.ioc.Collector;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.template.ioc.ReadFromWriteTo;

public class OverlayTask extends IocTask {
	// Overlay mapper members
	private OverlayProvider[] overlayProviders;

	public String OVERLAY_PROVIDER_SEQUENCE = "OVERLAY_PROVIDER_SEQUENCE";
	public String DEFAULT_OVERLAY_PROVIDER_SEQUENCE = "time";

	@Override
	protected void setup() {
		initReader(0, VideoFrame.class);
		initWriter(0, VideoFrame.class);

		try {
			startOverlays();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void startOverlays() throws Exception {
		String[] overlayProviderSequence = this.getTaskConfiguration().getString(
				this.OVERLAY_PROVIDER_SEQUENCE,
				this.DEFAULT_OVERLAY_PROVIDER_SEQUENCE).split("[,|]");

		this.overlayProviders = new OverlayProvider[overlayProviderSequence.length];

		for (int i = 0; i < overlayProviderSequence.length; i++) {
			String currProvider = overlayProviderSequence[i];

			if (currProvider.equals("time")) {
				this.overlayProviders[i] = new TimeOverlayProvider();
			} else if (currProvider.equals("logo")) {
				this.overlayProviders[i] = new LogoOverlayProvider(this.getTaskConfiguration());
			} else {
				throw new Exception(String.format(
						"Unknown overlay provider: %s", currProvider));
			}
		}

		// Start the overlay providers before consuming the stream
		for (final OverlayProvider overlayProvider : this.overlayProviders) {
			overlayProvider.start();
		}
	}

	@ReadFromWriteTo(readerIndex = 0, writerIndices = 0)
	public void overlay(VideoFrame frame, Collector<VideoFrame> out) {

		if (frame.isDummyFrame()) {
			out.collect(VideoFrame.createDummyFrame());
			out.flush();
			return;
		}

		if (!frame.isEndOfStreamFrame()) {
			// Apply overlays to frame
			for (final OverlayProvider overlayProvider : this.overlayProviders) {
				if (overlayProvider != null) {
					final VideoOverlay videoOverlay = overlayProvider
							.getOverlayForStream(frame.streamId);

					if (videoOverlay != null) {
						videoOverlay.draw(frame);
					}
				}
			}
		} else {
			for (final OverlayProvider overlayProvider : this.overlayProviders) {
				overlayProvider.dropOverlayForStream(frame.streamId);
			}
		}

		out.collect(frame);

		if (frame.isEndOfStreamFrame()) {
			out.flush();
		}
	}

	@Override
	protected void shutdown() {
		for (final OverlayProvider overlayProvider : this.overlayProviders) {
			overlayProvider.stop();
		}
	}
}
