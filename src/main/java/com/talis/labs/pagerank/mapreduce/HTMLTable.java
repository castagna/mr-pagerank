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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HTMLTable extends Configured implements Tool {

	private static final boolean PIXELS_ONLY = false ;
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: HTMLTable <input path> <rows> <columns>") ;
			return -1 ;
		}
		
		int rows = Integer.parseInt(args[1]) ;
		int columns = Integer.parseInt(args[2]) ;
		
		String[][] pageranks = new String[rows][columns] ;
		ArrayList<String> correct = new ArrayList<String>() ;
		
		FileSystem fs = FileSystem.get(getConf()) ;
		PrintWriter out = new PrintWriter(fs.create(new Path(args[0] + File.separator + "pageranks.html")).getWrappedStream()) ;

		{
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0] + File.separator + "sorted-pageranks" + File.separator + "part-00000")))) ;
			for (int i = 0; i < rows; i++) {
				StringTokenizer st = new StringTokenizer(in.readLine()) ;
				correct.add(st.nextToken()) ;
			}
			in.close() ;
		}

		
		for (int j = 0; j < columns; j++) {
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0] + File.separator + "sorted-pageranks-" + j + ".dat")))) ;
			for (int i = 0; i < rows; i++) {
				pageranks[i][j] =  in.readLine() ;
			}
			in.close() ;
		}

		out.println("<table cellspacing=\"2\" cellpadding=\"2\">") ;
		
		if (PIXELS_ONLY) {
			for (int i = 0; i < rows; i++) {
				out.print("<tr>") ;
				for (int j = 0; j < columns; j++) {
					String color = "#cc0000" ; 
					if ( correct.get(i).equals(pageranks[i][j])) {
						color = "#00cc00" ;
					} else if ( correct.contains(pageranks[i][j]) ) {
						color = "#cccc00" ;
					}
					out.print("<td bgcolor=\"" + color + "\"><img src=\"pixel.gif\" width=4 height=4 /></td>") ;
				}
				out.println("</tr>") ;
			}			
		} else {
			out.print("<tr><td>&nbsp;</td>") ;
			for (int j = 0; j < columns; j++) {
				out.print("<td>" + (j+1) + "</td>") ;
			}
			out.println("</tr>") ;
			
			for (int i = 0; i < rows; i++) {
				out.print("<tr><td>" + (i+1) + "</td>") ;
				for (int j = 0; j < columns; j++) {
					String color = "#cc0000" ; 
					if ( correct.get(i).equals(pageranks[i][j])) {
						color = "#00cc00" ;
					} else if ( correct.contains(pageranks[i][j]) ) {
						color = "#cccc00" ;
					}

					out.print("<td bgcolor=\"" + color + "\">" + pageranks[i][j] + "</td>") ;
				}
				out.println("</tr>") ;
			}	
		}

		out.println("</table>") ;

		out.close() ;

		return 0 ;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new HTMLTable(), args);
		System.exit(exitCode);
	}

}
