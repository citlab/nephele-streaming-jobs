package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

public enum FlvTagState {
	EMPTY,
	FOUND_HEADER,
	FOUND_BODY,
	FULL
}