package de.tuberlin.cit.test.queuebehavior.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

public class NumberRecord extends AbstractTaggableRecord {

	public enum Primeness {
		PRIME, NOT_PRIME, UNKNOWN
	};

	private BigInteger number;
	
	private Primeness primeness;

	public NumberRecord() {
		this.primeness = Primeness.UNKNOWN;
	}

	public BigInteger getNumber() {
		return number;
	}

	public void setNumber(BigInteger number) {
		this.number = number;
	}

	public Primeness getPrimeness() {
		return primeness;
	}

	public void setPrimeness(Primeness primeness) {
		this.primeness = primeness;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		byte[] numByte = this.number.toByteArray();
		out.writeInt(numByte.length);
		out.write(numByte);
		out.writeUTF(this.primeness.toString());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		byte[] numByte = new byte[in.readInt()];
		in.readFully(numByte);
		this.number = new BigInteger(numByte);
		this.primeness = Primeness.valueOf(in.readUTF());
	}

}
