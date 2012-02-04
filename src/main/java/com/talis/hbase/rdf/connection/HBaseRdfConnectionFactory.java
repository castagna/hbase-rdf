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

package com.talis.hbase.rdf.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.talis.hbase.rdf.HBaseRdfException;

public class HBaseRdfConnectionFactory 
{
    public static HBaseRdfConnection create( HBaseRdfConnectionDesc desc ) { return worker( desc ) ; }

    public static HBaseRdfConnection create( String configFile, boolean isAssemblerFile )
    { 
    	if( isAssemblerFile )
    	{
    		HBaseRdfConnectionDesc desc = HBaseRdfConnectionDesc.read( configFile ) ;
    		return create( desc ) ;
    	}
    	else
        	return new HBaseRdfConnection( configFile ) ;    		
    }

    public static HBaseRdfConnection create( Configuration config ) { return new HBaseRdfConnection( config ) ; }
    
    private static HBaseRdfConnection worker( HBaseRdfConnectionDesc desc )
    { return makeHBaseConnection( desc ) ; }

    private static HBaseRdfConnection makeHBaseConnection( HBaseRdfConnectionDesc desc )
    {
        HBaseRdfConnection c = new HBaseRdfConnection( createHBaseConfiguration( desc.getConfig() ) ) ;
        return c ;
    }

    public static HBaseAdmin createHBaseAdmin( HBaseRdfConnectionDesc desc )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( desc.getConfig() ) ;
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new HBaseRdfException( "HBase exception while creating admin" ) ; }     	
    }
    
    public static HBaseAdmin createHBaseAdmin( String configFile )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( new Path( configFile ) ) ;
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new HBaseRdfException( "HBase exception while creating admin" ) ; }     	    	
    }
    
    public static HBaseAdmin createHBaseAdmin( Configuration config )
    {
    	try { return new HBaseAdmin( config ) ; } 
    	catch ( Exception e ) { throw new HBaseRdfException( "HBase exception while creating admin" ) ; }    	
    }
    
    public static Configuration createHBaseConfiguration( String configFile )
    {
    	Configuration config = HBaseConfiguration.create() ;
    	config.addResource( new Path( configFile ) ) ;
    	return config ;
    }
}