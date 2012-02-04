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

package com.talis.hbase.rdf.test.junit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;
import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.iterator.Transform;

import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.test.HBaseRdfTestSetup;
import com.talis.hbase.rdf.util.Pair;

public abstract class ParamAllStores
{
    // Make into Object[]{String,Store} lists just for JUnit. 
    static Transform<Pair<String, StoreDesc>, Object[]> fix = new Transform<Pair<String, StoreDesc>, Object[]>()
    {
        public Object[] convert( Pair<String, StoreDesc> item )
        { return new Object[]{ item.car(), item.cdr() } ; }
    } ;

    // Build once and return the same for parametrized types each time.
    // Connections are slow to create.
    static Collection<Object[]> data = null ;
    static 
    {
        List<Pair<String, StoreDesc>> x = new ArrayList<Pair<String, StoreDesc>>() ;
        x.addAll( StoreList.stores( HBaseRdfTestSetup.storeList ) ) ;
        data = Iter.iter( x ).map( fix ).toList() ;
    }
    
    // ----
    
    // Each Object[] becomes the arguments to the class constructor (with reflection)
    // Reflection is not sensitive to generic parameterization (it's type erasure) 
    @Parameters public static Collection<Object[]> data() { return data ; }
    
    protected final String name ;
    protected final Store store ;
    
    public ParamAllStores( String name, Store store )
    {
        this.name = name ;
        this.store = store ;
    }
}
