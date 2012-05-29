/**
 * Copyright 2011 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobij.examples;

import com.nicta.scoobij.*;
import java.io.*;
import com.nicta.scoobij.io.text.*;


public class WordCount {

	public static void main(String[] args) throws java.io.IOException {

		String[] a = Scoobi.withHadoopArgs(args);

		String inputPath = null;
		String outputPath = null;

		if (a.length == 0) {
			if (!new File("output-dir").mkdir()) {
				System.err
						.println("Could not make output-dir for results. Perhaps it already exists (and you should delete/rename the old one)");
				System.exit(-1);
			}

			outputPath = "output-dir/word-results";
			inputPath = "output-dir/all-words.txt";

			// generate 5000 random words (with high collisions) and save at
			// fileName
			generateWords(inputPath, 5000);

		} else if (a.length == 2) {
			inputPath = a[0];
			outputPath = a[1];
		} else {
			System.err
					.println("Expecting input and output path, or no arguments at all.");
			System.exit(-1);
		}

		// Firstly we load up all the (new-line-seperated) words into a DList
		DList<String> lines = TextInput.fromTextFile(inputPath);

		DTable<String, Integer> nums = lines.tableMap(
				new TableMapper<String, String, Integer>() {
					public KeyValue<String, Integer> apply(String s) {
						return new KeyValue<String, Integer>(s, new Integer(1));
					}
				}, WireFormats.string(), WireFormats.integer());

		DGroupedTable<String, Integer> grouped = nums.groupByKey(
				WireFormats.string(), WireFormats.integer());

		DTable<String, Integer> reduced = grouped.combine(
				new Combiner<Integer>() {
					@Override
					public Integer apply(Integer a, Integer b) {
						return new Integer(a.intValue() + b.intValue());
					}
				}, WireFormats.string(), WireFormats.integer());

		// We can evalute this, and write it to a text file
		Scoobi.persist(TextOutput.toTextFile(reduced, outputPath, false,
				WireFormats.string(), WireFormats.integer()));
	}

	/*
	 * Write 'count' random words to the file 'filename', with a high amount of
	 * collisions
	 */
	static private void generateWords(String filename, int count)
			throws java.io.IOException {
		FileWriter fstream = new FileWriter(filename);
		scala.util.Random r = new scala.util.Random();

		// we will start off by generating count/10 different "words"
		String[] words = new String[count / 10];

		for (int i = 0; i < words.length; ++i) {
			words[i] = randomWord(r);
		}

		// and now we will pick 'count' of them to write to file
		for (int i = 0; i < count; ++i) {
			fstream.write(words[r.nextInt(words.length)]);
		}

		fstream.close();
	}

	// function to make a 5 leter random "word"
	static private String randomWord(scala.util.Random r) {
		int wordLength = 5;
		StringBuilder sb = new StringBuilder(wordLength + 1);
		for (int i = 0; i < wordLength; ++i) {
			sb.append((char) ('A' + r.nextInt('Z' - 'A')));
		}
		sb.append('\n');
		return sb.toString();
	}
}
