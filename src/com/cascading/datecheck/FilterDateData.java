package com.cascading.datecheck;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class FilterDateData extends BaseOperation implements Filter{

	DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	String dateString = "2001-05-03";
	Date actualDate = null;	
	Date filterDate = null;
	@Override
	public boolean isRemove(FlowProcess fp, FilterCall fc) {
		
		TupleEntry te = fc.getArguments();
		
		long longDate = te.getLong("f3");
		Date date = new Date(longDate);
		String actual = sdf.format(date);
	
		try {
			filterDate = sdf.parse(dateString);
			actualDate = sdf.parse(actual);
		} catch (ParseException e) {
			e.printStackTrace();
		} 

		return actualDate.compareTo(filterDate) != 0 ; 
	}
	
	
	
}