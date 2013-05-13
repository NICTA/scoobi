package com.nicta.scoobi.testing.script

import org.specs2.specification.Groups

abstract class NictaHadoop extends com.nicta.scoobi.testing.NictaHadoop with org.specs2.specification.script.SpecificationLike with Groups

abstract class NictaSimpleJobs extends NictaHadoop with com.nicta.scoobi.testing.SimpleJobs
