package de.tuberlin.cit.livescale.job.util.overlay;

public interface OverlayProvider {
	
	public void start();
	
	public void stop();

	public void dropOverlayForStream(long streamId);
	
	public VideoOverlay getOverlayForStream(long streamId);
}
