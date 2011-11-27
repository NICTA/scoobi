package com.nicta.scoobij.impl;

public class WithHadoopArgExtractor extends
		scala.runtime.AbstractFunction1<String[], scala.runtime.BoxedUnit> {
	public String[] extract(String[] args) {
		newArgs = null;
		com.nicta.scoobi.Scoobi.withHadoopArgs(args, this);
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
