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

public class CheckingDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static final Text NONE = new Text("dangling") ;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString()) ;
		boolean first = true ;
		String page = null ;
		Set<String> links = new HashSet<String>() ;
		while (st.hasMoreTokens()) {
			if (first) {
				page = st.nextToken() ;
				context.write(new Text(page), NONE) ;
				first = false ;
			} else {
				String token = st.nextToken() ;
				// this is to remove self-references and duplicates
				if ( (links.add(token)) && (!page.equals(token)) ) {
					context.write(new Text(page), new Text(token)) ;
					context.write(new Text(token), NONE) ; // this is for exposing implicit dangling pages					
				}
			}
		}
	}

}