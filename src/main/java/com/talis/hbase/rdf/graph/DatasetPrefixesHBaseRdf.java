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

package com.talis.hbase.rdf.graph;

import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.impl.PrefixMappingImpl;
import com.hp.hpl.jena.sparql.core.DatasetPrefixStorage;

public class DatasetPrefixesHBaseRdf implements DatasetPrefixStorage 
{
	static final String unamedGraphURI = "" ;
	
	@Override
	public PrefixMapping getPrefixMapping() 
	{ return getPrefixMapping( unamedGraphURI ) ; }

	@Override
	public PrefixMapping getPrefixMapping( String graphName ) 
	{ 
		//TODO: Uses default prefix mapping implementation
		return new PrefixMappingImpl() ;  
	}
	
	@Override
	public Set<String> graphNames() 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertPrefix( String graphName, String prefix, String uri ) 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void loadPrefixMapping( String graphName, PrefixMapping pmap ) 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public String readByURI( String graphName, String uriStr ) 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String readPrefix( String graphName, String prefix ) 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> readPrefixMap( String graphName ) 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeFromPrefixMap( String graphName, String prefix, String uri ) 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void close() 
	{
		// TODO Auto-generated method stub	
	}

	@Override
	public void sync() 
	{
		// TODO Auto-generated method stub	
	}

	@Override
	public void sync( boolean force ) 
	{
		// TODO Auto-generated method stub	
	}
}