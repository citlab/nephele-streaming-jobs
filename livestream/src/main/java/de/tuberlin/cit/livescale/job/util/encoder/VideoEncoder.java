package de.tuberlin.cit.livescale.job.util.encoder;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.awt.image.BufferedImage;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IPacket;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.IRational;
import com.xuggle.xuggler.IStream;
import com.xuggle.xuggler.IStreamCoder;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.video.ConverterFactory;
import com.xuggle.xuggler.video.IConverter;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.record.VideoFrame;

public class VideoEncoder {

	public static final String ENCODER_OUTPUT_FORMAT = "ENCODER_OUTPUT_FORMAT";

	private static final Log LOG = LogFactory.getLog(VideoEncoder.class);

	private long lastTimestamp;

	private int lastFrameId;

	private final long streamId;

	private final long groupId;

	private IContainer container;

	private IStreamCoder streamCoder;

	private IConverter videoConverter;

	private int nextPacketId = 0;

	private long encoderCreationTime;

	private long pipelineLag;

	private TCPReceiver receiver;

	public VideoEncoder(long streamId, long groupId) {
		this.encoderCreationTime = System.currentTimeMillis();
		this.streamId = streamId;
		this.groupId = groupId;
		this.lastTimestamp = -1;
		this.lastFrameId = -1;
		this.pipelineLag = 0;
	}

	public Packet init(String outputFormatString) throws InterruptedException {
		IContainerFormat format = IContainerFormat.make();
		if (format.setOutputFormat(outputFormatString, null, null) < 0) {
			LOG.error(String.format("Failed to set output container format for stream with id %d", streamId));
		}

		this.receiver = new TCPReceiver();
		this.receiver.startAndWaitUntilReady();
		this.container = IContainer.make();
		if (this.container.open(this.receiver.getUrl(), IContainer.Type.WRITE, format) < 0) {
			LOG.error(String.format("Failed to open new container for stream with id %d", streamId));
		}

		// if (this.container.open(this.protocolHandler, IContainer.Type.WRITE, format) < 0) {
		// LOG.error(String.format("Failed to open new container for stream with id %d", streamId));
		// }

		IStream stream = this.container.addNewStream(ICodec.ID.CODEC_ID_H264);
		this.streamCoder = stream.getStreamCoder();
		this.streamCoder.setWidth(480);
		this.streamCoder.setHeight(360);
		this.streamCoder.setProperty("profile", "baseline");

		IRational frameRate = IRational.make(30, 1);
		this.streamCoder.setFrameRate(frameRate);
		IRational timeBase = IRational.make(frameRate.getDenominator(), frameRate.getNumerator());
		this.streamCoder.setTimeBase(timeBase);
		this.streamCoder.setBitRate(500000);
		this.streamCoder.setPixelType(IPixelFormat.Type.YUV420P);

		// number of frames between key frames (intra-coded pictures in
		// MPEG-speak)
		this.streamCoder.setNumPicturesInGroupOfPictures(25);

		if (this.streamCoder.open(null, null) < 0) {
			LOG.error(String.format("Failed to open stream encoder for stream with id %d", streamId));
		}

		if (this.container.writeHeader() < 0) {
			LOG.error(String.format("Failed to write container header for stream with id %d", streamId));
		}

		return createPacketToEmit(System.currentTimeMillis());
	}

	public Packet encodeFrame(VideoFrame frame) {
		if (this.lastTimestamp >= frame.timestampInNanos) {
			LOG.warn(String.format(
				"Warning: Frame reordering has occured! oldframeId: %d | newframeId: %d | oldts: %d | newts: %d",
				this.lastFrameId, frame.frameId, this.lastTimestamp, frame.timestampInNanos));
			frame.timestampInNanos = this.lastTimestamp + 10000000; // add 10ms
		}
		this.lastTimestamp = frame.timestampInNanos;
		this.lastFrameId = frame.frameId;

		BufferedImage converted = convertToType(frame.frameImage, BufferedImage.TYPE_3BYTE_BGR);

		if (this.videoConverter == null) {
			this.videoConverter = ConverterFactory.createConverter(
				ConverterFactory.findDescriptor(frame.frameImage),
				this.streamCoder.getPixelType(),
				this.streamCoder.getWidth(), this.streamCoder.getHeight(),
				frame.frameImage.getWidth(), frame.frameImage.getHeight());
		}

		IVideoPicture videoPicture = this.videoConverter.toPicture(converted,
			MICROSECONDS.convert(frame.timestampInNanos, TimeUnit.NANOSECONDS));

		IPacket xugglePacket = IPacket.make();
		if (this.streamCoder.encodeVideo(xugglePacket, videoPicture, 0) < 0) {
			LOG.error(String.format("Error when encoding frame %d in stream %d", frame.frameId, frame.streamId));
		}

		Packet encodedFrame = null;
		if (xugglePacket.isComplete()) {
			if (this.container.writePacket(xugglePacket) < 0) {
				LOG.error(String.format("Error when writing encoded packet of frame %d in stream %d", frame.frameId,
					frame.streamId));
			}
			encodedFrame = createPacketToEmit(frame.getTimestamp());
			if (frame.frameId % 17 == 0) {
				refreshPipelineLag(frame.timestampInNanos / 1000000);
			}
		}
		return encodedFrame;

	}

	private void refreshPipelineLag(long frameTimestampInMillis) {
		long timePassedSinceEncoderCreation = System.currentTimeMillis() - encoderCreationTime;
		this.pipelineLag = timePassedSinceEncoderCreation - frameTimestampInMillis;
		LOG.debug(String.format("Pipeline lag for stream %d measured at encoder %d\n", streamId, pipelineLag));
	}

	private Packet createPacketToEmit(long timestamp) {
		Packet toEmit = null;

		container.flushPackets();
		if (receiver.hasDataAvailableForReading()) {
			byte[] packetData = receiver.readAvailableData();
			toEmit = new Packet(this.streamId, this.groupId, this.getNextPacketId(), packetData, timestamp);
		}
		
		return toEmit;
	}

	public BufferedImage convertToType(BufferedImage sourceImage, int targetType) {
		BufferedImage image;

		// if the source image is already the target type, return the source
		// image
		if (sourceImage.getType() == targetType) {
			image = sourceImage;
		} else {
			image = new BufferedImage(sourceImage.getWidth(),
					sourceImage.getHeight(), targetType);
			image.getGraphics().drawImage(sourceImage, 0, 0, null);
		}

		return image;
	}

	public Packet closeVideoEncoder() throws InterruptedException {
		this.container.writeTrailer();
		this.streamCoder.close();
		if (this.container.close() < 0) {
			LOG.error(String.format("Error when closing encoder container in stream %d", this.streamId));
		}
		receiver.join();
		Packet toReturn = createPacketToEmit(System.currentTimeMillis());
		
		return toReturn;
	}

	private int getNextPacketId() {
		int toReturn = nextPacketId;
		nextPacketId++;
		return toReturn;
	}

	public long getStreamId() {
		return streamId;
	}

	public long getGroupId() {
		return groupId;
	}
}
