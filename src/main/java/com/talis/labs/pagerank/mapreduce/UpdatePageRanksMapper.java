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
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpdatePageRanksMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		String page = st.nextToken();
		double pagerank = Double.parseDouble(st.nextToken());
		st.nextToken(); // previous pagerank 
		if (st.hasMoreTokens()) {
			ArrayList<String> links = new ArrayList<String>();
			while (st.hasMoreTokens()) {
				links.add(st.nextToken());
			}

			StringBuffer sb = new StringBuffer().append("pages\t").append(pagerank).append("\t");
			for (String link : links) {
				context.write(new Text(link), new Text("pagerank\t" + (pagerank / links.size())));
				sb.append(link).append("\t");
			}			
			context.write(new Text(page), new Text(sb.toString()));
		} else {
			context.write(new Text(page), new Text("pages\t" + pagerank));
		}
	}

}
