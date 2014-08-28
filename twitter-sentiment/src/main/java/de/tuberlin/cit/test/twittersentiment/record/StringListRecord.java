package de.tuberlin.cit.test.twittersentiment.record;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StringListRecord extends AbstractTaggableRecord {
	private List<String> list;

	public StringListRecord() {
	}

	public StringListRecord(List<String> list) {
		this.list = list;
	}

	public List<String> getList() {
		return list;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(list.size());
		for (String s : list) {
			out.writeUTF(s);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		int size = in.readInt();
		list = new ArrayList<String>(size);
		for (int i = 0; i < size; i++) {
			list.add(in.readUTF());
		}
	}
}
