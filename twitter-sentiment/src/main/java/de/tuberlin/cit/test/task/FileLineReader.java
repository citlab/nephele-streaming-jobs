package de.tuberlin.cit.test.task;

import de.tuberlin.cit.test.record.StringRecord;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;

import java.util.Iterator;

/**
 * A file line reader reads the associated file input splits line by line and outputs the lines as json records.
 */
public class FileLineReader extends AbstractFileInputTask {
	private RecordWriter<StringRecord> output;

	@Override
	public void registerInputOutput() {
		output = new RecordWriter<StringRecord>(this, StringRecord.class);
	}

	@Override
	public void invoke() throws Exception {

		final Iterator<FileInputSplit> splitIterator = getFileInputSplits();

		while (splitIterator.hasNext()) {

			final FileInputSplit split = splitIterator.next();

			long start = split.getStart();
			long length = split.getLength();

			final FileSystem fs = FileSystem.get(split.getPath().toUri());
			final FSDataInputStream fdis = fs.open(split.getPath());

			final LineReader lineReader = new LineReader(fdis, start, length, 1024 * 1024);

			byte[] line;
			while ((line = lineReader.readLine()) != null) {
				output.emit(new StringRecord(line));
			}

			lineReader.close();
			fdis.close();
		}
	}
}
