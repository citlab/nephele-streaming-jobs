package de.tuberlin.cit.livescale.job.util.decoder;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xuggle.mediatool.IMediaReader;
import com.xuggle.mediatool.MediaToolAdapter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.mediatool.event.IVideoPictureEvent;
import com.xuggle.xuggler.IContainer;
import com.xuggle.xuggler.IContainerFormat;
import com.xuggle.xuggler.IError;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.record.VideoFrame;

public final class VideoDecoder extends MediaToolAdapter {

	private static final Log LOG = LogFactory.getLog(VideoDecoder.class);

	private static IContainerFormat FLV_FORMAT;

	static {
		for (IContainerFormat format : IContainerFormat
				.getInstalledInputFormats()) {
			if (format.getInputFormatShortName().equals("flv")) {
				FLV_FORMAT = format;
			}
		}
	}

	private long streamId;

	private long groupId;

	private IContainer container;

	private IMediaReader mediaReader;

	private ByteBufferedURLProtocolHandler protoHandler;

	private VideoFrame decodedFrame;

	private int frameCounter;

	private long lastPacketTimestamp;

	public VideoDecoder(long streamId, long groupId) throws IOException {
		this.streamId = streamId;
		this.groupId = groupId;
		this.container = IContainer.make();
		this.mediaReader = ToolFactory.makeReader(container);
		this.mediaReader.setBufferedImageTypeToGenerate(BufferedImage.TYPE_3BYTE_BGR);
		this.mediaReader.addListener(this);
		this.protoHandler = new ByteBufferedURLProtocolHandler("DecoderBuffer");
	}

	public void closeDecoder() {
		mediaReader.close();
		container.close();
		protoHandler = null;
		LOG.debug("Closed decoder");
	}

	@Override
	public void onVideoPicture(IVideoPictureEvent videoPictureEvent) {
		decodedFrame = new VideoFrame(streamId,
			groupId,
			frameCounter,
			videoPictureEvent.getTimeStamp(TimeUnit.NANOSECONDS),
			videoPictureEvent.getImage(),
			lastPacketTimestamp);
		frameCounter++;
	}

	public VideoFrame decodePacket(Packet packet) throws IOException {
		this.lastPacketTimestamp = packet.getTimestamp();

		byte[] packetData = packet.getData();
		int written = protoHandler.write(packetData, packetData.length);
		if (written != packetData.length) {
			LOG.error(String.format("Could not fully consume packet %d due to lack of buffer space\n",
				packet.getPacketIdInStream()));
		}

		VideoFrame toReturn = null;
		if (packet.getPacketIdInStream() == 0) {
			openContainer();
		} else {
			toReturn = decodeNextFrame(packet);
		}

		return toReturn;
	}

	private VideoFrame decodeNextFrame(Packet packet) throws IOException {
		if (packet.getStreamId() != streamId) {
			LOG.error(String.format("Packet does not belong to required stream %d (belongs to %d instead)",
				streamId,
				packet.getStreamId()));
		}

		if (packet.getGroupId() != groupId) {
			LOG.error(String.format(
				"Packet %d of stream %d does not belong to required group %d! (belongs to group %d instead)",
				packet.getPacketIdInStream(),
				streamId,
				groupId,
				packet.getGroupId()));
		}

		IError error = mediaReader.readPacket();
		if (error != null) {
			throw new IOException(String.format("Got error %s in packet id %d\n", error.toString(),
					packet.getPacketIdInStream()));
		}

		VideoFrame toReturn = null;
		if (decodedFrame == null) {
			LOG.warn(String.format("Did not get frame for packet id %d\n", packet.getPacketIdInStream()));
		} else {
			toReturn = decodedFrame;
			decodedFrame = null;
		}
		return toReturn;
	}

	private void openContainer() throws IOException {
		if (container.open(protoHandler, IContainer.Type.READ, FLV_FORMAT, false, false) < 0) {
			throw new IOException("failed to open");
		}
	}

	public VideoFrame createEndOfStreamFrame() {
		LOG.debug("Creating end of stream frame");
		VideoFrame frame = new VideoFrame(streamId, groupId, 0, 0, null, System.currentTimeMillis());
		frame.markAsEndOfStreamFrame();
		return frame;
	}
}
