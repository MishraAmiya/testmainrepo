package com.cascading.datecheck;


import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

class FilterDate {
	public static void main(String[] args) {
		
		DateType dt = new DateType("yyyy-MM-dd");
		Fields sourceField = new Fields("f1", "f2", "f3").applyTypes(
				String.class, Double.class, dt);
		Scheme sch = new TextDelimited(sourceField, false, ",");
		Tap source = new Hfs(sch, 
				"C:/Users/amiyam/Desktop/CascadingJavaClass/lingualdata.txt");
		Tap sink = new Hfs(
				sch,
				"C:/Users/amiyam/Desktop/CascadingJavaClass/LingualOutput",
				SinkMode.REPLACE);

		Pipe p = new Pipe("Date filter ");

		p = new Each(p, sourceField, new FilterDateData());

		FlowDef fd = FlowDef.flowDef().addSource(p, source)
				.addTailSink(p, sink);

		new Hadoop2MR1FlowConnector().connect(fd).complete();
	}
}