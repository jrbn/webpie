package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import data.Tree.Node;
import data.Tree.Node.Rule;

public class TripleSource implements WritableComparable<TripleSource> {

    final boolean ENABLE_TREE = false;

    // 7th bit: transitive active or transitive passive
    // 6th bit: already filtered or not
    // 5th-1st bit: derivation rule
    private byte derivation = 0;
    private int step;
    Tree.Node.Builder node = Tree.Node.newBuilder();

    // All possible derivations
    /*** CAN HAVE AT MOST 32 RULES (because we reserve 5 bytes) ***/
    // public static final byte INPUT = 0;

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
	// step = in.readInt();

	byte[] array = new byte[in.readInt()];
	in.readFully(array);
	node.clear();
	node.mergeFrom(array);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.write(derivation);
	// out.writeInt(step);
	//
	byte[] array = node.build().toByteArray();
	out.writeInt(array.length);
	out.write(array);
    }

    public boolean isTripleDerived() {
	return (derivation & 31) != 0;
    }

    public int getStep() {
	return node.getStep();
    }

    public void setStep(int step) {
	node.setStep(step);
    }

    public void setRule(Rule value) {
	node.setRule(value);
    }

    @Override
    // Method not implemented
    public int compareTo(TripleSource o) {
	return 0;
    }

    public void addChild(Node child) {
	node.addChildren(child);
    }

    public Node getHistory() {
	return node.build();
    }

    public void clearChildren() {
	node.clearChildren();
    }

    public void removeChildren(int index) {
	node.removeChildren(index);
    }

    public int getNChildren() {
	return node.getChildrenCount();
    }

    public Rule getRule() {
	return node.getRule();
    }
}