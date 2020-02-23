package com.home.bigdata.model;

import java.io.Serializable;

public class Employee implements Serializable {
	
	private int empId;
	
	private String empName;
	
	private int deptId;
	
	

	public Employee(int empId, String empName, int deptId) {
		super();
		this.empId = empId;
		this.empName = empName;
		this.deptId = deptId;
	}

	public int getEmpId() {
		return empId;
	}

	public String getEmpName() {
		return empName;
	}

	public int getDeptId() {
		return deptId;
	}
	
	

}
