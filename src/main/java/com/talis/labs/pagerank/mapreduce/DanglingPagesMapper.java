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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DanglingPagesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text KEY_NAME = new Text("dangling") ;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString()) ;
		if (st.hasMoreTokens()) {
			st.nextToken() ;
			if (st.hasMoreTokens()) {
				double pagerank = Double.parseDouble(st.nextToken()) ;
				st.nextToken() ; // previous pagerank
				if (!st.hasMoreTokens()) {
					context.write(KEY_NAME, new DoubleWritable(pagerank)) ;							
				}
			}
		}
	}

}