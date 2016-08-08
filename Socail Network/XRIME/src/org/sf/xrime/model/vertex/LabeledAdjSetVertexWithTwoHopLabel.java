package org.sf.xrime.model.vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.label.Labels;

public class LabeledAdjSetVertexWithTwoHopLabel extends
		AdjSetVertexWithTwoHopLabel implements Labelable {

	private Labels labels = null;

	static {
		// Register writable factory for this class.
		WritableFactories.setFactory(LabeledAdjSetVertexWithTwoHopLabel.class,
				new WritableFactory() {
					public Writable newInstance() {
						return new LabeledAdjSetVertexWithTwoHopLabel();
					}
				});
	}

	public LabeledAdjSetVertexWithTwoHopLabel() {
		super();
		labels = new Labels();
	}

	public LabeledAdjSetVertexWithTwoHopLabel(String id) {
		super(id);
		labels = new Labels();
	}

	public LabeledAdjSetVertexWithTwoHopLabel(
			LabeledAdjSetVertexWithTwoHopLabel set) {
		super(set);
		labels = (Labels) set.labels.clone();
	}

	@Override
	public Writable getLabel(String name) {
		return labels.getLabel(name);
	}

	@Override
	public void setLabel(String name, Writable value) {
		labels.setLabel(name, value);
	}

	@Override
	public void removeLabel(String name) {
		labels.removeLabel(name);
	}

	/**
	 * Get the string-valued label with specified name.
	 * 
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		return labels.getStringLabel(name);
	}

	/**
	 * Add or modify a string-valued label.
	 * 
	 * @param name
	 * @param value
	 */
	public void setStringLabel(String name, String value) {
		labels.setStringLabel(name, value);
	}

	@Override
	public void clearLabels() {
		labels.clearLabels();
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		String super_str = super.toString();
		buffer.append(super_str.substring(0, super_str.length() - 1));
		buffer.append(", ");
		buffer.append(labels.toString());
		buffer.append(">");
		return buffer.toString();
	}

	@Deprecated
	@Override
	public void fromString(String encoding) {
		// do nothing
	}

	public Object clone() {
		return new LabeledAdjSetVertexWithTwoHopLabel(this);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		labels.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		labels.write(out);
	}

}
