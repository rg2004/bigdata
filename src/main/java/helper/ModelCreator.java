package helper;

import java.util.ArrayList;
import java.util.List;

import com.home.bigdata.model.Department;
import com.home.bigdata.model.Employee;

public class ModelCreator {
	
	private ModelCreator() {
		
	}

	
public static List<Employee>  getEmployees(){
		
		List<Employee> list = new ArrayList<Employee>();	
		Employee o1=new Employee(2067,"Rohit",100);
		Employee o2=new Employee(2068,"Manoj",200);
		list.add(o1);
		list.add(o2);	
		return list;
	}
	
 public static List<Department>  getDepartments(){
		
		List<Department> list = new ArrayList<Department>();	
		Department o1=new Department(100,"Market Risk");
		Department o2=new Department(200,"Credit Risk");
		list.add(o1);
		list.add(o2);	
		return list;
	}
}
