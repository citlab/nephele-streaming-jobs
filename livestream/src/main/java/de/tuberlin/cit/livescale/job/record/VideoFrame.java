package de.tuberlin.cit.livescale.job.record;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

public final class VideoFrame extends AbstractTaggableRecord {

	public long streamId;

	/**
	 * A negative value marks the end of the stream and signals that cleanup
	 * actions need to be taken.
	 */
	public int frameId;

	public long groupId;

	public long timestamp;

	/**
	 * Contains the timestamp of the frame inside the video stream (i.e. in
	 * "video time", not wall clock time).
	 */
	public long timestampInNanos;

	public BufferedImage frameImage;

	private final static ComponentColorModel COLOR_MODEL = new ComponentColorModel(
			ColorSpace.getInstance(ColorSpace.CS_sRGB), false, false,
			Transparency.OPAQUE, DataBuffer.TYPE_BYTE);

	public VideoFrame() {
		this.frameId = -1;
		this.streamId = -1;
		this.groupId = -1;
		this.timestamp = -1;
	}

	public static VideoFrame createDummyFrame() {
		return new VideoFrame(-1, -1, -2, 0, null, System.currentTimeMillis());
	}

	public VideoFrame(long streamId, long groupId, int frameId,
			long frameTimestampInNanos, BufferedImage frameImage, long timestamp) {
		this.streamId = streamId;
		this.groupId = groupId;
		this.frameId = frameId;
		this.timestampInNanos = frameTimestampInNanos;
		this.frameImage = frameImage;
		this.timestamp = timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);

		out.writeLong(streamId);
		out.writeInt(frameId);
		out.writeLong(groupId);
		out.writeLong(timestamp);

		if (!isEndOfStreamFrame() && !isDummyFrame()) {
			out.writeLong(timestampInNanos);
			serializeImage(frameImage, out);
		}
	}

	private void serializeImage(BufferedImage bufferedImage, DataOutput out)
			throws IOException {

		serializeSampleModel(bufferedImage.getSampleModel(), out);

		final DataBuffer db = bufferedImage.getData().getDataBuffer();
		if (!(db instanceof DataBufferByte)) {
			throw new IllegalArgumentException(
					"DataBuffer must be of type DataBufferByte");
		}

		// Banks
		final DataBufferByte dbb = (DataBufferByte) db;
		final byte[][] bankData = dbb.getBankData();
		out.writeInt(bankData.length);
		for (int i = 0; i < bankData.length; ++i) {
			out.writeInt(bankData[i].length);
			out.write(bankData[i]);
		}

		// Size
		out.writeInt(dbb.getSize());

		// Offsets
		final int[] offsets = dbb.getOffsets();
		out.writeInt(offsets.length);
		for (int i = 0; i < offsets.length; ++i) {
			out.writeInt(offsets[i]);
		}

	}

	private void serializeSampleModel(SampleModel sm, DataOutput out)
			throws IOException {

		if (!(sm instanceof PixelInterleavedSampleModel)) {
			throw new IllegalArgumentException(
					"Argument sm must be of type java.awt.image.PixelInterleavedSampleModel");
		}

		final PixelInterleavedSampleModel pism = (PixelInterleavedSampleModel) sm;

		out.writeInt(pism.getDataType());
		out.writeInt(pism.getWidth());
		out.writeInt(pism.getHeight());
		out.writeInt(pism.getPixelStride());
		out.writeInt(pism.getScanlineStride());

		final int[] bandOffsets = pism.getBandOffsets();
		out.writeInt(bandOffsets.length);
		for (int i = 0; i < bandOffsets.length; ++i) {
			out.writeInt(bandOffsets[i]);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {

		super.read(in);

		streamId = in.readLong();
		frameId = in.readInt();
		groupId = in.readLong();
		timestamp = in.readLong();

		if (!isEndOfStreamFrame() && !isDummyFrame()) {
			timestampInNanos = in.readLong();
			frameImage = deserializeImage(in);
		}
	}

	public boolean isDummyFrame() {
		return this.frameId == -2;
	}

	private BufferedImage deserializeImage(DataInput in) throws IOException {

		final SampleModel sampleModel = deserializeSampleModel(in);

		// Banks
		final byte[][] bankData = new byte[in.readInt()][];
		for (int i = 0; i < bankData.length; ++i) {
			bankData[i] = new byte[in.readInt()];
			in.readFully(bankData[i]);
		}

		// Size
		final int size = in.readInt();

		// Offsets
		final int[] offsets = new int[in.readInt()];
		for (int i = 0; i < offsets.length; ++i) {
			offsets[i] = in.readInt();
		}

		final DataBufferByte dbb = new DataBufferByte(bankData, size, offsets);

		final WritableRaster wr = Raster.createWritableRaster(sampleModel, dbb,
				null);

		return new BufferedImage(COLOR_MODEL, wr, false, null);
	}

	private SampleModel deserializeSampleModel(DataInput in) throws IOException {

		final int dataType = in.readInt();
		final int width = in.readInt();
		final int height = in.readInt();
		final int pixelStride = in.readInt();
		final int scanlineStride = in.readInt();

		final int numberOfBandOffsets = in.readInt();
		final int[] bandOffsets = new int[numberOfBandOffsets];
		for (int i = 0; i < numberOfBandOffsets; ++i) {
			bandOffsets[i] = in.readInt();
		}

		return new PixelInterleavedSampleModel(dataType, width, height,
				pixelStride, scanlineStride, bandOffsets);
	}

	public boolean isEndOfStreamFrame() {
		return frameId == -1;
	}

	public void markAsEndOfStreamFrame() {
		frameId = -1;
		frameImage = null;
		timestampInNanos = -1;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
