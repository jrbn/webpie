package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompressedTriple implements WritableComparable<CompressedTriple>, Serializable {

	private static final long serialVersionUID = 7578125506159351415L;

	public long subject = 0;
	public long predicate = 0;
	public long object = 0;
	public boolean isObjectLiteral = false;

	public CompressedTriple(long s, long p, long o, boolean literal) {
		this.subject = s;
		this.predicate = p;
		this.object = o;
		this.isObjectLiteral = literal;
	}

	public CompressedTriple() {
	}

	public static long readResource(DataInput in) throws IOException {

		// Read the machine number
		int machineNumber = 0;
		int b = in.readByte() & 0xFF;
		int i = b & 3;
		machineNumber = b >> 2;
		for (int m = 0; m < i; m++) {
			machineNumber += (in.readByte() & 0xFF) << m * 8 + 6;
		}

		// Read the counter
		long counter = 0;
		b = in.readByte() & 0xFF;
		i = b & 7;
		counter = b >> 3;
		for (int m = 0; m < i; m++) {
			counter += (in.readByte() & 0xFF) << m * 8 + 5;
		}

		return ((long) machineNumber << 40) + counter;
	}

	public static void writeResource(DataOutput out, long resource)
			throws IOException {

		// Encode the machine number
		int machineNumber = (int) (resource >> 40);
		byte n = 0;
		int maxValue = 63;
		while (machineNumber > maxValue) {
			maxValue <<= 8;
			n++;
		}
		machineNumber <<= 2;
		machineNumber += n;
		out.write(machineNumber);
		for (int m = 0; m < n; ++m) {
			machineNumber >>= 8;
			out.write(machineNumber);
		}

		// Encode the counter
		long counter = resource & 0xFFFFFFFFFFl;
		n = 0;
		long maxValueCounter = 31;
		while (counter > maxValueCounter) {
			maxValueCounter <<= 8;
			n++;
		}
		counter <<= 3;
		counter += n;
		out.write((int) (counter));
		for (int m = 0; m < n; ++m) {
			counter >>= 8;
			out.write((int) counter & 0xFF);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		 subject = readResource(in);
		 predicate = readResource(in);
		 object = readResource(in);

		byte flags = in.readByte();
		isObjectLiteral = (flags >> 1) == 1;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		 writeResource(out, subject);
		 writeResource(out, predicate);
		 writeResource(out, object);

		byte flag = 0;
		if (isObjectLiteral)
			flag = (byte) (flag | 0x2);
		out.writeByte(flag);

	}

	public String toString() {
		return subject + " " + predicate + " " + object;
	}

	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public int compareTo(CompressedTriple o) {
		if (subject == o.subject && predicate == o.predicate
				&& object == o.object)
			return 0;
		else {
			if (subject > o.subject) {
				return 1;
			} else {
				if (subject == o.subject) { // Check the predicate
					if (predicate > o.predicate) {
						return 1;
					} else {
						if (predicate == o.predicate) { // Check the object
							if (object > o.object) {
								return 1;
							} else {
								return -1;
							}
						}
						return -1;
					}
				}
				return -1;
			}
		}
	}

	public boolean equals(Object triple) {
		if (compareTo((CompressedTriple) triple) == 0)
			return true;
		else
			return false;
	}

	public long getSubject() {
		return subject;
	}

	public void setSubject(long subject) {
		this.subject = subject;
	}

	public long getPredicate() {
		return predicate;
	}

	public void setPredicate(long predicate) {
		this.predicate = predicate;
	}

	public long getObject() {
		return object;
	}

	public void setObject(long object) {
		this.object = object;
	}

	public boolean isObjectLiteral() {
		return isObjectLiteral;
	}

	public void setObjectLiteral(boolean isObjectLiteral) {
		this.isObjectLiteral = isObjectLiteral;
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(CompressedTriple.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1 - 1, b2, s2, l2 - 1);
		}
	}

	static {
		WritableComparator.define(CompressedTriple.class, new Comparator());
	}
}
