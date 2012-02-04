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

package com.talis.hbase.rdf.test;

import com.talis.hbase.rdf.HBaseRdfFactory;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.hash.StoreHash;
import com.talis.hbase.rdf.layout.hybrid.StoreHybrid;
import com.talis.hbase.rdf.layout.indexed.StoreIndexed;
import com.talis.hbase.rdf.layout.simple.StoreSimple;
import com.talis.hbase.rdf.layout.verticalpartitioning.StoreVP;
import com.talis.hbase.rdf.layout.vpindexed.StoreVPIndexed;
import com.talis.hbase.rdf.store.LayoutType;

public class StoreCreator 
{
	private static StoreSimple ss ;
	private static StoreVP svp ;
	private static StoreIndexed sin ;
	private static StoreVPIndexed svpin ;
	private static StoreHybrid shy ;
	private static StoreHash sha ;
	
	public static Store getStoreSimple()
	{
		if( ss == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutSimple, "ss" ) ;
			ss = new StoreSimple( conn, desc ) ;
			ss.getTableFormatter().format() ;
		}
		else 
			ss.getTableFormatter().truncate() ;
		return ss ;
	}
	
	public static Store getStoreVerticallyPartitioned()
	{
		if( svp == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutVertPart, "vps" ) ;
			svp = new StoreVP( conn, desc ) ;
			svp.getTableFormatter().format() ;
		}
		else 
			svp.getTableFormatter().truncate() ;
		return svp ;
	}	
	
	public static Store getStoreIndexed()
	{
		if( sin == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutIndexed, "in" ) ;
			sin = new StoreIndexed( conn, desc ) ;
			sin.getTableFormatter().format() ;
		}
		else 
			sin.getTableFormatter().truncate() ;
		return sin ;
	}

	public static Store getStoreVPIndexed()
	{
		if( svpin == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutVPIndexed, "vpin" ) ;
			svpin = new StoreVPIndexed( conn, desc ) ;
			svpin.getTableFormatter().format() ;
		}
		else 
			svpin.getTableFormatter().truncate() ;
		return svpin ;
	}

	public static Store getStoreHybrid()
	{
		if( shy == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutHybrid, "hy" ) ;
			shy = new StoreHybrid( conn, desc ) ;
			shy.getTableFormatter().format() ;
		}
		else 
			shy.getTableFormatter().truncate() ;
		return shy ;
	}

	public static Store getStoreHash()
	{
		if( sha == null )
		{
			HBaseRdfConnection conn = HBaseRdfFactory.createConnection( "/Cloud/Hbase/hbase-0.89.20100924/conf/hbase-site.xml", false ) ;
			StoreDesc desc = new StoreDesc( LayoutType.LayoutSimple, "ha" ) ;
			sha = new StoreHash( conn, desc ) ;
			sha.getTableFormatter().format() ;
		}
		else 
			sha.getTableFormatter().truncate() ;
		return sha ;
	}

}