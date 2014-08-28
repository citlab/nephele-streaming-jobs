package de.tuberlin.cit.test.twittersentiment.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringRecord extends AbstractTaggableRecord {
	private String str;

	public StringRecord() {
		this("");
	}

	public StringRecord(byte[] bytes) {
		this(new String(bytes));
	}

	public StringRecord(String str) {
		this.str = str;
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(str);
	}

	@Override
	public void read(final DataInput out) throws IOException {
		super.read(out);
		str = out.readUTF();
	}

	@Override
	public String toString() {
		return str;
	}
}
