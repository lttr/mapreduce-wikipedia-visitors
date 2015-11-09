package cz.lttr.hadoop.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NameDayPair implements WritableComparable<NameDayPair> {

	private Text name = new Text();
	private Text day = new Text();

	public NameDayPair(String name, String day) {
		this.name.set(name);
		this.day.set(day);
	}

	public NameDayPair() {
	}

	public void write(DataOutput out) throws IOException {
		name.write(out);
		day.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		day.readFields(in);
	}

	public int compareTo(NameDayPair nameDayPair) {
		int compareValue = this.name.toString().compareTo(nameDayPair.getName());
		if (compareValue == 0) {
			compareValue = this.day.toString().compareTo(nameDayPair.getDay());
		}
		return compareValue;
	}

	public String getName() {
		return name.toString();
	}

	public void setName(String name) {
		this.name.set(name);
	}

	public String getDay() {
		return day.toString();
	}

	public void setDay(String day) {
		this.day.set(day);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		NameDayPair that = (NameDayPair) obj;
		if (name == null) {
			if (that.name != null)
				return false;
		} else {
			if (!name.equals(that.name))
				return false;
		}

		if (day == null) {
			if (that.day != null)
				return false;
		} else {
			if (!day.equals(that.day))
				return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int resultPageName = name != null ? name.hashCode() : 0;
		int resultPageCount = day != null ? day.hashCode() : 0;
		return 31 * resultPageName + resultPageCount;
	}

	@Override
	public String toString() {
		return name + "\t" + day;
	}

}
