package com.home.bigdata.model;

import java.io.Serializable;

public class Department implements Serializable{
	
	int deptId;
	
	String deptName;

	public Department(int deptId, String deptName) {
		super();
		this.deptId = deptId;
		this.deptName = deptName;
	}

	public int getDeptId() {
		return deptId;
	}

	public String getDeptName() {
		return deptName;
	}
	

}
