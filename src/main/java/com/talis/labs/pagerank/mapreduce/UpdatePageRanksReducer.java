/*
 * Copyright © 2011 Talis Systems Ltd.
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

package com.talis.labs.pagerank.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UpdatePageRanksReducer extends Reducer<Text, Text, Text, Text> {

	private long count;
	private double dangling;
	private double d = 0.85;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		count = Long.parseLong(context.getConfiguration().get("pagerank.count"));
		dangling = Double.parseDouble(context.getConfiguration().get("pagerank.dangling"));
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double pagerank = 0;
		double previous_pagerank = -1;
		StringBuffer links = new StringBuffer();
		
		for (Text value : values) {
			StringTokenizer st = new StringTokenizer(value.toString());
			if ("pages".equals(st.nextToken())) {
				previous_pagerank = Double.parseDouble(st.nextToken());
				while (st.hasMoreTokens()) {
					links.append(st.nextToken()).append("\t");
				}
			} else {
				pagerank += Double.parseDouble(st.nextToken());
			}
		}
		pagerank = d*(pagerank) + d*dangling/count + (1-d)/count;
		context.write(key, new Text(pagerank + "\t" + previous_pagerank + "\t" + links.toString()));		
	}

}