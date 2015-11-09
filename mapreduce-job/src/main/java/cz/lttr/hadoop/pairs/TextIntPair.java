package cz.lttr.hadoop.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntPair implements WritableComparable<TextIntPair> {

	private Text text = new Text();
	private IntWritable integer = new IntWritable();

	public TextIntPair(String text, int integer) {
		this.text.set(text);
		this.integer.set(integer);
	}

	public TextIntPair() {
	}

	public void write(DataOutput out) throws IOException {
		text.write(out);
		integer.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		integer.readFields(in);
	}

	public int compareTo(TextIntPair textIntPair) {
		int compareValue = this.text.toString().compareTo(textIntPair.getText());
		if (compareValue == 0) {
			compareValue = Integer.compare(this.integer.get(), textIntPair.getInt());
		}
		return compareValue;
	}

	public String getText() {
		return text.toString();
	}

	public void setText(String text) {
		this.text.set(text);
	}

	public int getInt() {
		return integer.get();
	}

	public void setInt(int integer) {
		this.integer.set(integer);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		TextIntPair that = (TextIntPair) obj;
		if (text == null) {
			if (that.text != null)
				return false;
		} else {
			if (!text.equals(that.text))
				return false;
		}

		if (integer == null) {
			if (that.integer != null)
				return false;
		} else {
			if (!integer.equals(that.integer))
				return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int resultText = text != null ? text.hashCode() : 0;
		int resultInt = integer != null ? integer.hashCode() : 0;
		return 31 * resultText + resultInt;
	}

	@Override
	public String toString() {
		return text + "\t" + integer;
	}
}
