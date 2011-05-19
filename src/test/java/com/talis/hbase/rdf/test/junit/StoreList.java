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

package com.talis.hbase.rdf.test.junit;

import static org.openjena.atlas.lib.StrUtils.strjoinNL ;

import java.util.ArrayList;
import java.util.List;

import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.iterator.Transform;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.FileManager;
import com.talis.hbase.rdf.HBaseRdf;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.store.StoreFactory;
import com.talis.hbase.rdf.util.Pair;
import com.talis.hbase.rdf.util.Vocab;

public class StoreList
{
    static Property description = Vocab.property( HBaseRdf.namespace, "description" ) ;
    static Property list = Vocab.property( HBaseRdf.namespace, "list" ) ;
    static Resource storeListClass = Vocab.property( HBaseRdf.namespace, "StoreList" ) ;
    
    static boolean formatStores = false ;
    static String queryString = strjoinNL
            (   
             "PREFIX hbaserdf: <http://rdf.hbase.talis.com/2011/hbase-rdf#>" ,
             "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" ,
             "PREFIX rdfs:     <http://www.w3.org/2000/01/rdf-schema#>" ,
             "PREFIX list:     <http://jena.hpl.hp.com/ARQ/list#>" ,
             "SELECT ?desc ?label" ,
             "{ [] rdf:type hbaserdf:StoreList ;" ,
             "     hbaserdf:list ?l ." ,
             "  ?l list:member [ rdfs:label ?label ; hbaserdf:description ?desc ]",
            "}") ;
    
    // Not Java's finest hour ...
    static Transform<Pair<String, String>, Pair<String, StoreDesc>> t1 = new  Transform<Pair<String, String>, Pair<String, StoreDesc>>()
    {
        public Pair<String, StoreDesc> convert(Pair<String, String> pair)
        {
            return new Pair<String, StoreDesc>( pair.car(), StoreDesc.read( pair.cdr() ) ) ;
        }
    } ;

    static Transform<Pair<String, StoreDesc>, Pair<String, Store>> t2 = new Transform<Pair<String, StoreDesc>, Pair<String, Store>>()
    {
        public Pair<String, Store> convert( Pair<String, StoreDesc> pair )
        {
            Store store = testStore( pair.cdr() ) ;
            return new Pair<String, Store>( pair.car(), store ) ;
        }
    } ;
    
    public static Store testStore( StoreDesc desc )
    {
        Store store = StoreFactory.create( desc ) ;
        if ( formatStores || inMem( store ) )
            store.getTableFormatter().create() ;
        return store ;
    }
    
    public static boolean inMem( Store store ) { return false ; }
    
    public static List<Pair<String, StoreDesc>> stores( String fn )
    {
        List<Pair<String, String>> x = storesByQuery( fn ) ;
        List<Pair<String, StoreDesc>> z = Iter.iter( x ).map( t1 ).toList() ;
        //List<Pair<String, Store>> z = Iter.iter(x).map(t1).map(t2).toList() ;
        return z ;
    }
    
    public static List<Pair<String, StoreDesc>> storeDesc( String fn )
    {
        List<Pair<String, String>> x = storesByQuery( fn ) ;
        List<Pair<String, StoreDesc>> y = Iter.iter( x ).map( t1 ).toList() ;
        return y ;
    }
    
    private static List<Pair<String, String>> storesByQuery( String fn )
    {
        Model model = FileManager.get().loadModel( fn ) ;
        List<Pair<String, String>> data = new ArrayList<Pair<String, String>>();
        Query query = QueryFactory.create( queryString ) ;
        QueryExecution qExec = QueryExecutionFactory.create( query, model ) ;
        try 
        {
            ResultSet rs = qExec.execSelect() ;
            
            for ( ; rs.hasNext() ; )
            {
                QuerySolution qs = rs.nextSolution() ;
                String label = qs.getLiteral( "label" ).getLexicalForm() ;
                String desc = qs.getResource( "desc" ).getURI() ;
                data.add( new Pair<String, String>( label, desc ) ) ;
            }
        } finally { qExec.close() ; }
        return data ;
    }
}