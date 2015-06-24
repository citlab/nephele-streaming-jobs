package de.tuberlin.cit.livescale.job.util.overlay;

public class TimeOverlayProvider implements OverlayProvider {

	private final TimeVideoOverlay overlay;
	
	public TimeOverlayProvider() {
		this.overlay = new TimeVideoOverlay();
	}
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stop() {
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#dropOverlayForStream(long)
	 */
	@Override
	public void dropOverlayForStream(long streamId) {
		// do nothing
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.cit.livescale.job.util.overlay.OverlayProvider#getOverlayForStream(long)
	 */
	@Override
	public VideoOverlay getOverlayForStream(long streamId) {
		return this.overlay;
	}

}
