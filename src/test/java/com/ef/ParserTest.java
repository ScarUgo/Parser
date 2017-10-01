package com.ef;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class ParserTest {

	public static void main(String[] args) {
	    Result result = JUnitCore.runClasses(DatabaseAccessTest.class);	    
	    
	    if(result.getFailureCount() == 0){
	    	System.out.println("All tests were successful");
	    }
	    else{
	    	for (Failure failure : result.getFailures()) {
	  	      System.out.println(failure.toString());
	  	    }
	    }
	   
	  }
}
