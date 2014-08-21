package de.tuberlin.cit.test.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringRecord extends AbstractTaggableRecord {
	private String str;

	public StringRecord() {
		this.str = "";
	}

	public StringRecord(String str) {
		this.str = str;
	}

	public StringRecord(byte[] bytes) {
		this.str = new String(bytes);
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
