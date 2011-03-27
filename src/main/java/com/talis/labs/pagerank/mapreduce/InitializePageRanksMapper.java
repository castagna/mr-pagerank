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
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitializePageRanksMapper extends Mapper<LongWritable, Text, Text, Text> {

	private double pagerank;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		long count = Long.parseLong(context.getConfiguration().get("pagerank.count"));
		pagerank = 1.0 / count;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		Text page = null;
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		Set<String> links = new HashSet<String>();
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			if (first) {
				page = new Text(token);
				sb.append(pagerank).append("\t"); // current pagerank
				sb.append(pagerank).append("\t"); // previous pagerank
				first = false;
			} else {
				// to remove duplicated links and self-references
				if ((links.add(token)) && (!page.equals(token))) {
					sb.append(token).append("\t");
				}
			}
		}
		
		context.write(page, new Text(sb.toString()));
	}
	
}
