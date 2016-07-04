package com.cascading.datecheck;

import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

public class FilterCascading {
	public static void main(String[] args) {
		Logger log = LoggerFactory.getLogger(FilterCascading.class);
		if (args.length != 2) {
			log.info("two command line arguments required");
		} else {
			final String inputpath = args[0];
			final String outputpath = args[1];
			log.info("input path : " + inputpath);
			log.info("output path: " + outputpath);
			DateType datetype = new DateType("yyyy-MM-dd");
			Fields sourceField = new Fields("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
					"f13", "f14", "f15", "f16", "f17", "f18", "f19", "f20", "f21", "f22", "f23", "f24", "f25", "f26",
					"f27", "f28", "f29", "f30", "f31", "f32", "f33", "f34", "f35", "f36", "f37", "f38", "f39", "f40",
					"f41", "f42", "f43", "f44", "f45", "f46", "f47", "f48", "f49", "f50", "f51", "f52", "f53", "f54",
					"f55", "f56", "f57", "f58", "f59", "f60", "f61", "f62", "f63", "f64", "f65", "f66", "f67", "f68",
					"f69", "f70", "f71", "f72").applyTypes(String.class, datetype, datetype, BigDecimal.class,
							Boolean.class, Integer.class, Float.class, Long.class, Double.class, String.class, datetype,
							datetype, BigDecimal.class, Boolean.class, Integer.class, Float.class, Long.class,
							Double.class, String.class, datetype, datetype, BigDecimal.class, Boolean.class,
							Integer.class, Float.class, Long.class, Double.class, String.class, datetype, datetype,
							BigDecimal.class, Boolean.class, Integer.class, Float.class, Long.class, Double.class,
							String.class, datetype, datetype, BigDecimal.class, Boolean.class, Integer.class,
							Float.class, Long.class, Double.class, String.class, datetype, datetype, BigDecimal.class,
							Boolean.class, Integer.class, Float.class, Long.class, Double.class, String.class, datetype,
							datetype, BigDecimal.class, Boolean.class, Integer.class, Float.class, Long.class,
							Double.class, String.class, datetype, datetype, BigDecimal.class, Boolean.class,
							Integer.class, Float.class, Long.class, Double.class);
			Scheme sch = new TextDelimited(sourceField, false, ",");
			Tap source = new Hfs(sch, inputpath);
			Tap sink = new Hfs(sch, outputpath, SinkMode.REPLACE);
			Pipe p = new Pipe("Date filter ");
			p = new Each(p, sourceField, new FilterFunctionCascading());
			FlowDef fd = FlowDef.flowDef().addSource(p, source).addTailSink(p, sink);
			new Hadoop2MR1FlowConnector().connect(fd).complete();
		}
	}
}