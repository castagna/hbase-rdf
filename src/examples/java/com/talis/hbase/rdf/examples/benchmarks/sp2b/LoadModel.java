/*
 * Copyright © 2010, 2011, 2012 Talis Systems Ltd.
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

package com.talis.hbase.rdf.examples.benchmarks.sp2b;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import com.hp.hpl.jena.rdf.model.Model;
import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;

public class LoadModel 
{
	public static void main( String[] args ) throws FileNotFoundException, InterruptedException
	{
		int numOfRuns = new Integer( args[0] ).intValue() ;
		
		for( int i = 1 ; i <= numOfRuns ; i++ )
		{
			Store store = HBaseRdfFactory.connectStore( args[1] ) ;
			if( args[2].equals( "--format=true" ) ) store.getTableFormatter().format() ;
			Model model = HBaseRdfFactory.connectNamedModel( store, args[3] ) ;
			long startTime = System.nanoTime() ;
			model.read( new FileInputStream( args[4] ), null, "N3" ) ;
			model.close() ; //Needs to be explicit to accomplish bulk loading in HBase
			long endTime = System.nanoTime() ;
			System.out.println( "Time to load:: " + ( endTime - startTime ) * 1e-6 + " ms." ) ;
			System.out.println( "Total size:: " + store.getTotalSize() ) ;
			Thread.sleep( 10000 ) ;
		}
	}
}
