package com.nicta.scoobij.impl;

import com.nicta.scoobij.WireFormats;

import scala.runtime.BoxedUnit;

public class DObjectImpl {
	public static  com.nicta.scoobi.core.DObject<BoxedUnit> empty() {
		return com.nicta.scoobi.core.DObject$.MODULE$.apply(scala.runtime.BoxedUnit.UNIT,WireFormats.wireFormatUnit().typeInfo(), WireFormats.wireFormatUnit().wireFormat()  );
	}
}
