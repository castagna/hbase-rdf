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

import com.hp.hpl.jena.graph.Node;
import com.talis.hbase.rdf.layout.TableDescLayouts;

public interface TupleLoader
{
    /** Set table description */
    public void setTableDesc( TableDescLayouts tableDesc ) ;
    
    /** Get the table descriptions */
    public TableDescLayouts getTableDesc() ;
    
    /** Notify the start of a sequence of rows to load */
    public void start() ;
    
    /** Load a row - may not take place immediately
     *  but row object is free for reuse after calling this method.
     * @param row
     */
    public void load( Node... row ) ;
    
    /** Remove a row - may not take place immediately
     *  but row object is free for reuse after calling this method.
     * @param row
     */
    public void unload( Node... row ) ;

    /** Notify the finish of a sequence of rows to load.  
     * All data will have been loaded by the time this returns */ 
    public void finish() ;
    
    /** This TupleLoader is done with.
     * Do not use a TupleLoader after calling close().
     */
    public void close() ;
}