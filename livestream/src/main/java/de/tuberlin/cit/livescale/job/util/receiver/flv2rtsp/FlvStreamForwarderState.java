package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

public enum FlvStreamForwarderState {
	EMPTY,
	FOUND_FLV_HEADER,
	FOUND_FLV_META_TAG,
	BUFFERING_VIDEOTAGS
}
