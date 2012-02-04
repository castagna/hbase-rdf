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

package com.talis.hbase.rdf.store;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.sparql.core.DataSourceImpl;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.talis.hbase.rdf.HBaseRdf;
import com.talis.hbase.rdf.Store;

public class DatasetStore extends DataSourceImpl
{
	static { HBaseRdf.init() ; }
	
    public static Dataset create( Store store )
    { 
        DatasetGraph dsg = createDatasetGraph( store ) ;
        return new DatasetImpl( dsg ) ;
    }
  
    public static Dataset create( Store store, ReificationStyle style )
    { 
        DatasetGraph dsg = createDatasetGraph( store, style ) ;
        return new DatasetImpl( dsg ) ;
    }    
    
    public static DatasetGraph createDatasetGraph( Store store )
    { 
        return createDatasetStoreGraph( store, ReificationStyle.Standard ) ;
    }

    public static DatasetGraph createDatasetGraph( Store store, ReificationStyle style )
    { 
        return createDatasetStoreGraph( store, style ) ;
    }

    public static DatasetStoreHBaseRdfGraph createDatasetStoreGraph( Store store, ReificationStyle style )
    { 
        return new DatasetStoreHBaseRdfGraph( store, HBaseRdf.getContext().copy(), style ) ;
    }
}
