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

import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.hp.hpl.jena.util.FileManager;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.assembler.AssemblerVocab;

public class HBaseRdfConnectionDesc 
{
	private String config = null ;
	
    public static HBaseRdfConnectionDesc blank()
    { return new HBaseRdfConnectionDesc() ; }
    
    public static HBaseRdfConnectionDesc none()
    {
    	HBaseRdfConnectionDesc x = new HBaseRdfConnectionDesc() ;
        x.config = "none" ;
        return x ;
    }

    private HBaseRdfConnectionDesc() {}
    
    public static HBaseRdfConnectionDesc read( String filename )
    {
        Model m = FileManager.get().loadModel( filename ) ;
        return worker( m ) ;
    }
    
    public static HBaseRdfConnectionDesc read( Model m ) { return worker( m ) ; }
    
    private static HBaseRdfConnectionDesc worker( Model m )
    {
        Resource r = GraphUtils.getResourceByType( m, AssemblerVocab.HBaseConnectionAssemblerType ) ;
        if ( r == null )
            throw new HBaseRdfException( "Can't find connection description" ) ;
        HBaseRdfConnectionDesc desc = ( HBaseRdfConnectionDesc )AssemblerBase.general.open( r ) ;
        return desc ;
    }

    public void setConfig( String config ) { this.config = config; }
    
    public String getConfig() { return config; }    
}