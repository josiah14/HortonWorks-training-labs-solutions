package dbio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class Employee implements Writable, DBWritable {
	private String first;
	private String last;
	private String dept;
	private double salary;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		first = result.getString(1);
		last = result.getString(2);
		dept = result.getString(3);
		salary = result.getDouble(4);
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		stmt.setString(1, first);
		stmt.setString(2, last);
		stmt.setString(3, dept);
		stmt.setDouble(4, salary);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		last = in.readUTF();
		dept = in.readUTF();
		salary = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(last);
		out.writeUTF(dept);
		out.writeDouble(salary);
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getLast() {
		return last;
	}

	public void setLast(String last) {
		this.last = last;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

}
