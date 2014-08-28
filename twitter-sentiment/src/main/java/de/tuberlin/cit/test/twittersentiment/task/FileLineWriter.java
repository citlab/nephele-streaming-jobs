package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.StringRecord;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

/**
 * A file line writer reads string records its input gate and writes them to the associated output file.
 *
 * @author warneke
 */
public class FileLineWriter extends AbstractFileOutputTask {

	/**
	 * The record reader through which incoming string records are received.
	 */
	private RecordReader<StringRecord> input = null;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		Path outputPath = getFileOutputPath();

		FileSystem fs = FileSystem.get(outputPath.toUri());
		if (fs.exists(outputPath)) {
			FileStatus status = fs.getFileStatus(outputPath);

			if (status.isDir()) {
				outputPath = new Path(outputPath.toUri().toString() + "/file_" + getIndexInSubtaskGroup() + ".txt");
			}
		}

		final FSDataOutputStream outputStream = fs.create(outputPath, true);

		while (this.input.hasNext()) {

			StringRecord record = this.input.next();
			byte[] recordByte = (record.toString() + "\r\n").getBytes();
			outputStream.write(recordByte, 0, recordByte.length);
		}

		outputStream.close();

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<StringRecord>(this, StringRecord.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumNumberOfSubtasks() {
		// The default implementation always returns -1
		return -1;
	}
}
