package dbio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class SalaryData implements Writable, DBWritable {
	private String department;
	private double salary;
	private int mapkey;

	@Override
	public void readFields(ResultSet result) throws SQLException {
		department = result.getString(1);
		salary = result.getDouble(2);
		mapkey = result.getInt(3);
	}

	@Override
	public void write(PreparedStatement stmt) throws SQLException {
		stmt.setString(1, department);
		stmt.setDouble(2, salary);
		stmt.setInt(3, mapkey);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		department = in.readUTF();
		salary = in.readDouble();
		mapkey = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(department);
		out.writeDouble(salary);
		out.writeInt(mapkey);
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public double getSalary() {
		return salary;
	}

	public void setSalary(double salary) {
		this.salary = salary;
	}

	public int getMapkey() {
		return mapkey;
	}

	public void setMapkey(int mapkey) {
		this.mapkey = mapkey;
	}

	
}
