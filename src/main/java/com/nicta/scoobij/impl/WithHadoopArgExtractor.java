package com.nicta.scoobij.impl;

import com.nicta.scoobi.application.ScoobiConfiguration;
import org.apache.hadoop.conf.Configuration;
import scala.collection.immutable.Set$;

public class WithHadoopArgExtractor extends
		scala.runtime.AbstractFunction1<String[], scala.runtime.BoxedUnit> {
	public String[] extract(String[] args) {
		newArgs = null;
		new ScoobiConfiguration(new Configuration(), Set$.MODULE$.<String>empty(), Set$.MODULE$.<String>empty()).withHadoopArgs(args, this);
		assert (newArgs != null);
		return newArgs;

	}

	private String[] newArgs = null;

	@Override
	public scala.runtime.BoxedUnit apply(String[] args) {
		newArgs = args;
		return scala.runtime.BoxedUnit.UNIT;
	}
}
