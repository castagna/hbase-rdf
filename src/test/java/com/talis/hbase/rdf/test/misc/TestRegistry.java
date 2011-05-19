/*
 * Copyright © 2010 Talis Systems Ltd.
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

package com.talis.hbase.rdf.test.misc;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.talis.hbase.rdf.store.LayoutType;

public class TestRegistry 
{
	String[] layoutNames = { "layout1" , "layout2" } ;

	@Test public void reg_layout_1()
	{
		// Tests default configuration.
		for ( String s : layoutNames )
			assertNotNull( LayoutType.fetch( s ) ) ;
	}

	@Test public void reg_layout_2()
	{
		for ( String s : LayoutType.allNames() )
			assertNotNull( LayoutType.fetch( s ) ) ;
	}

	@Test public void reg_layout_3()
	{
		for ( LayoutType t : LayoutType.allTypes() )
			assertNotNull( LayoutType.fetch( t.getName() ) ) ;
	}
}
