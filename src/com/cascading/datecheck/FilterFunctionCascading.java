package com.cascading.datecheck;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class FilterFunctionCascading extends BaseOperation implements Filter {

	@Override
	public boolean isRemove(FlowProcess fp, FilterCall fc) {
		TupleEntry te = fc.getArguments();
		Boolean f5 = te.getBoolean("f5");
		return (f5 == true);
	}

}