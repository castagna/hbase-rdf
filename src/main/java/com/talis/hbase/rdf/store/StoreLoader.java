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

package com.talis.hbase.rdf.store;

import com.hp.hpl.jena.graph.Triple;

public interface StoreLoader
{
    public void startBulkUpdate() ;
	public void addTriple( Triple triple ) ;
    public void deleteTriple( Triple triple ) ;
    public void finishBulkUpdate() ;
    public void close() ;
    
    public void setUseThreading( boolean useThreading );
    public boolean getUseThreading();
	public void deleteAll();    
}
