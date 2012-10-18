package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TripleSource implements WritableComparable<TripleSource> {

	// 7th bit: transitive active or transitive passive
	// 6th bit: already filtered or not
	// 5th-1st bit: derivation rule
	byte derivation = 0;
	int step = 0;

	// All possible derivations
	/*** CAN HAVE AT MOST 32 RULES (because we reserve 5 bytes) ***/
	public static final byte INPUT = 0;
	public static final byte RDFS_SUBPROP_INHERIT = 1;
	public static final byte RDFS_SUBPROP_TRANS = 2;
	public static final byte RDFS_DOMAIN = 3;
	public static final byte RDFS_RANGE = 4;
	public static final byte RDFS_SUBCLASS_INHERIT = 5;
	public static final byte RDFS_SUBCLASS_TRANS = 6;
	public static final byte RDFS_SUBCLASS_SPECIAL = 7;

	public static final byte OWL_RULE_1 = 8;
	public static final byte OWL_RULE_2 = 9;
	public static final byte OWL_RULE_3 = 10;
	public static final byte OWL_RULE_4 = 11;
	public static final byte OWL_RULE_7 = 12;
	public static final byte OWL_RULE_8 = 13;
	public static final byte OWL_RULE_12A = 14;
	public static final byte OWL_RULE_12C = 15;
	public static final byte OWL_RULE_13A = 16;
	public static final byte OWL_RULE_13C = 17;
	public static final byte OWL_RULE_14A = 18;
	public static final byte OWL_RULE_14B = 19;
	public static final byte OWL_RULE_15 = 20;
	public static final byte OWL_RULE_16 = 21;

	public static final byte OWL2_RULE_PROPAXIOM = 22;
	public static final byte OWL2_RULE_HASKEY = 23;

	public void setAlreadyFiltered(boolean alreadyFiltered) {
		if (alreadyFiltered) {
			derivation |= 32;
		} else {
			derivation &= 95;
		}
	}

	public boolean isAlreadyFiltered() {
		return ((derivation >> 5) & 1) > 0;
	}

	public void setTransitivityActive(boolean isActive) {
		if (isActive) {
			derivation |= 64;
		} else {
			derivation &= 63;
		}
	}

	public boolean isTransitiveActive() {
		return ((derivation >> 6) & 1) > 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		derivation = in.readByte();
		step = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(derivation);
		out.writeInt(step);
	}

	public boolean isTripleDerived() {
		return (derivation & 31) != 0;
	}

	public int getStep() {
		return step;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public void setDerivation(byte ruleset) {
		derivation |= ruleset & 31;
	}

	public byte getDerivation() {
		return derivation;
	}

	@Override
	// Method not implemented
	public int compareTo(TripleSource o) {
		return 0;
	}
}