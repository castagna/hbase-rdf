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

import org.openjena.atlas.lib.Sync;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.modify.GraphStoreEvents;
import com.hp.hpl.jena.sparql.util.graph.GraphListenerBase;

public class UpdateListener extends GraphListenerBase 
{
	Sync sync ;

	public UpdateListener( Sync g ) { sync = g ; }

	@Override
	public void notifyEvent( Graph source, Object value ) 
	{	
		if ( value.equals( GraphStoreEvents.RequestStartEvent ) ) {}
		else if ( value.equals( GraphStoreEvents.RequestFinishEvent ) ) { sync.sync( false ) ; }
		super.notifyEvent( source, value ) ;
	}

	@Override
	protected void addEvent( Triple t ) {}

	@Override
	protected void deleteEvent( Triple t ) {}
}