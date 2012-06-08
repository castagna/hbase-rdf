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

package com.talis.hbase.rdf.layout.hybrid;

import com.talis.hbase.rdf.StoreDesc;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.layout.StoreBase;
import com.talis.hbase.rdf.layout.TableDescLayouts;
import com.talis.hbase.rdf.store.StoreFormatter;
import com.talis.hbase.rdf.store.StoreLoader;
import com.talis.hbase.rdf.store.StoreQueryRunner;

public class StoreBaseHybrid extends StoreBase
{
    public StoreBaseHybrid( HBaseRdfConnection connection, StoreDesc desc, StoreQueryRunner querier, StoreFormatter formatter, StoreLoader loader ) 
    {
        super( connection, desc, querier, formatter, loader, new TableDescLayouts( new TableDescHybrid().getTableDesc() ) ) ;
    }
}
