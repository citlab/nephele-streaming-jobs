package de.tuberlin.cit.livescale.job.util.receiver.flv2rtsp;

import java.util.LinkedList;

public class BufferedFlvFile {

	private final int REQUIRED_KEYFRAMES_IN_BUFFER = 3;

	private LinkedList<FlvTag> bufferedVideoTags;

	private int bufferedKeyframes;

	private boolean dropExcessFrames;

	private FlvFileHeader flvFileHeader;

	private FlvTag flvMetaTag;

	private FlvTag flvAvcConfigVideoTag;

	public BufferedFlvFile() {
		this.bufferedVideoTags = new LinkedList<FlvTag>();
		this.bufferedKeyframes = 0;
		this.dropExcessFrames = true;
	}

	public FlvFileHeader getFlvFileHeader() {
		return flvFileHeader;
	}

	public void setFlvFileHeader(FlvFileHeader flvFileHeader) {
		this.flvFileHeader = flvFileHeader;
	}

	public FlvTag getFlvMetaTag() {
		return flvMetaTag;
	}

	public void setFlvMetaTag(FlvTag flvMetaTag) {
		this.flvMetaTag = flvMetaTag;
	}

	public FlvTag getFlvAvcConfigVideoTag() {
		return flvAvcConfigVideoTag;
	}

	public void setFlvAvcConfigVideoTag(FlvTag flvAvcConfigVideoTag) {
		this.flvAvcConfigVideoTag = flvAvcConfigVideoTag;
	}

	public void enqueueFlvTag(FlvTag tag) {
		if (!tag.isFull() || !tag.isVideoTag()) {
			throw new RuntimeException("Only full video tags can be added");
		}

		synchronized (bufferedVideoTags) {
			bufferedVideoTags.addFirst(tag);
			if (tag.isVideoKeyframe()) {
				bufferedKeyframes++;
				dropExcessFramesIfNecessary();
			}
			bufferedVideoTags.notify();
		}
	}

	/**
	 * Blocks until the next tag is available and returns it.
	 * 
	 * @return
	 * @throws InterruptedException
	 *         If interrupted while being blocked.
	 */
	public FlvTag dequeueFlvTag() throws InterruptedException {
		synchronized (bufferedVideoTags) {
			while (bufferedVideoTags.isEmpty()) {
				System.out.println("Buffer is empty.");
				bufferedVideoTags.wait();
			}
			FlvTag tag = bufferedVideoTags.removeLast();
			if (tag.isVideoKeyframe()) {
				bufferedKeyframes--;
			}

			return tag;
		}
	}

	private void dropExcessFramesIfNecessary() {
		if (dropExcessFrames) {
			dropExcessInterframes();
			while (bufferedKeyframes >= REQUIRED_KEYFRAMES_IN_BUFFER + 1) {
				bufferedVideoTags.removeLast();
				bufferedKeyframes--;
				dropExcessInterframes();
			}
		}
	}

	private void dropExcessInterframes() {
		while (bufferedVideoTags.getLast().isVideoInterframe()) {
			bufferedVideoTags.removeLast();
		}
	}

	public void setDropExcessFrames(boolean dropExcessFrames) {
		synchronized (bufferedVideoTags) {
			this.dropExcessFrames = dropExcessFrames;
			if (dropExcessFrames) {
				dropExcessFramesIfNecessary();
			}
		}
	}

	public void waitUntilBufferFull() throws InterruptedException {
		synchronized (bufferedVideoTags) {
			while (!isBufferFull()) {
				bufferedVideoTags.wait();
			}
		}
	}

	private boolean isBufferFull() {
		return flvFileHeader != null
			&& flvFileHeader.isFull()
			&& flvMetaTag != null
			&& flvMetaTag.isFull()
			&& flvAvcConfigVideoTag != null
			&& flvAvcConfigVideoTag.isFull()
			&& bufferedKeyframes >= REQUIRED_KEYFRAMES_IN_BUFFER;
	}
}
