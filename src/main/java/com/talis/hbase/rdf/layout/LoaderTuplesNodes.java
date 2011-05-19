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

package com.talis.hbase.rdf.layout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.talis.hbase.rdf.HBaseRdfException;
import com.talis.hbase.rdf.Store;
import com.talis.hbase.rdf.connection.HBaseRdfConnection;
import com.talis.hbase.rdf.store.StoreInformationHolder;
import com.talis.hbase.rdf.store.StoreLoaderPlus;
import com.talis.hbase.rdf.store.TableDesc;
import com.talis.hbase.rdf.store.TupleLoader;

public class LoaderTuplesNodes extends StoreInformationHolder implements StoreLoaderPlus
{
	private static Logger log = LoggerFactory.getLogger( LoaderTuplesNodes.class ) ;
	private boolean initialized = false ;
	boolean threading = true ; // Do we want to thread?
	Thread commitThread = null ; // The loader thread
	final static TupleChange flushSignal = new TupleChange() ; // Signal to thread to commit
	final static TupleChange finishSignal = new TupleChange() ; // Signal to thread to finish
	ArrayBlockingQueue<TupleChange> queue ; // Pipeline to loader thread
	AtomicReference<Throwable> threadException ; // Placeholder for problems thrown in the thread
	Object threadFlushing = new Object(); // We lock on this when flushing

	Map<TableDesc[], TupleLoader> tupleLoaders ;
	TupleLoader currentLoader ;

	int count ;
	int chunkSize = 20000 ;
	
 	private Class<? extends TupleLoader> tupleLoaderClass ;

	private Store store ;

	public LoaderTuplesNodes( String storeName, HBaseRdfConnection connection, Class<? extends TupleLoader> tupleLoaderClass )
	{
		super( storeName, connection ) ;
		this.tupleLoaderClass = tupleLoaderClass ;
	}

	public void setStore( Store store ) { this.store = store ; }

	public void startBulkUpdate() 		{ init() ; }

	public void finishBulkUpdate() 		{ flushTriples() ; }

	/**
	 * Close this loader and finish the thread (if required)
	 *
	 */
	 public void close()
	 {
		 if ( !initialized ) return ;

		 try
		 {
			 if ( threading && commitThread.isAlive() )
			 {
				 queue.put( finishSignal ) ;
				 commitThread.join() ;
			 }
			 else
				 flushTriples() ;
		 }
		 catch ( Exception e ) 
		 {
			 log.error( "Problem closing loader: " + e.getMessage() ) ; 
			 throw new HBaseRdfException( "Problem closing loader", e ) ;
		 }
		 finally
		 {
			 for( TupleLoader loader: this.tupleLoaders.values() ) loader.close() ;
			 this.initialized = false ;
			 this.commitThread = null ;
			 this.queue = null ;
			 this.tupleLoaderClass = null ;
			 this.tupleLoaders = null ;
		 }
	 }

	 public void addTriple( Triple triple ) { updateStore( new TupleChange( true, store.getTablesDesc(), triple.getSubject(), triple.getPredicate(), triple.getObject() ) ) ; }

	 public void deleteTriple( Triple triple ) { updateStore( new TupleChange( false, store.getTablesDesc(), triple.getSubject(), triple.getPredicate(), triple.getObject() ) ) ; }

	 public void addQuad( Node g, Node s, Node p, Node o ) { updateStore( new TupleChange( true, store.getTablesDesc(), g, s, p, o ) ) ; }

	 public void addTuple( TableDescLayouts t, Node... nodes ) { updateStore( new TupleChange( true, t, nodes ) ) ; }

	 public void deleteQuad( Node g, Node s, Node p, Node o ) { updateStore( new TupleChange( false, store.getTablesDesc(), g, s, p, o ) ) ; }

	 public void deleteTuple( TableDescLayouts t, Node... nodes ) { updateStore( new TupleChange( false, t, nodes ) ) ; }

	 public void deleteAll() { updateStore( new TupleChange( false, store.getTablesDesc() ) ) ; }

	 public void deleteAll( Node graph ) { updateStore( new TupleChange( false, store.getTablesDesc(), graph ) ) ; }

	 static class TupleChange 
	 {
		 public Node[] tuple ;
		 public boolean toAdd ;
		 public TableDescLayouts table ;

		 public TupleChange( boolean toAdd, TableDescLayouts table, Node... tuple ) 
		 {
			 this.tuple = tuple ;
			 this.toAdd = toAdd ;
			 this.table = table ;
		 }

		 public TupleChange() 
		 {
			 tuple = null ;
			 table = null ;
			 toAdd = false ;
		 }
	 }

	 private void updateStore( TupleChange tuple )
	 {   
		 if ( threading )
		 {
			 checkThreadStatus() ;
			 try { queue.put( tuple ) ; }
			 catch ( InterruptedException e )
			 {
				 log.error( "Issue adding to queue: " + e.getMessage() ) ;
				 throw new HBaseRdfException( "Issue adding to queue" + e.getMessage(), e ) ;
			 }
		 }
		 else
			 updateOneTuple( tuple ) ;
	 }

	 /**
	  * Flush remain triples in queue to database. If threading this blocks until flush is complete.
	  */
	 private void flushTriples()
	 {
		 if ( threading )
		 {
			 if ( !commitThread.isAlive() ) throw new HBaseRdfException( "Thread has died" ) ;
			 // finish up threaded load
			 try 
			 {
				 synchronized( threadFlushing ) 
				 {
					 queue.put( flushSignal ) ;
					 threadFlushing.wait() ;
				 }
			 }
			 catch ( InterruptedException e )
			 {
				 log.error( "Problem sending flush signal: " + e.getMessage() ) ;
				 throw new HBaseRdfException( "Problem sending flush signal", e) ;
			 }
			 checkThreadStatus() ;
		 }
		 else
			 commitTuples() ;
	 }

	 private void init()
	 {
		 if ( initialized ) return ;

		 tupleLoaders = new HashMap<TableDesc[], TupleLoader>() ;
		 currentLoader = null ;

		 count = 0 ;

		 if( threading )
		 {
			 queue = new ArrayBlockingQueue<TupleChange>( chunkSize ) ;
			 threadException = new AtomicReference<Throwable>() ;
			 threadFlushing = new AtomicBoolean() ;
			 commitThread = new Thread( new Commiter() ) ;
			 commitThread.setDaemon( true ) ;
			 commitThread.start() ;
			 log.debug( "Threading started" ) ;
		 }
		 initialized = true ;
	 }

	 private void checkThreadStatus()
	 {
		 Throwable e = threadException.getAndSet( null ) ;
		 if ( e != null )
		 {
			 if ( e instanceof RuntimeException )
				 throw (RuntimeException) e;
			 else
				 throw new HBaseRdfException( "Loader thread exception", e ) ;
		 }
		 if( !commitThread.isAlive() )
			 throw new HBaseRdfException( "Thread has died" ) ;
	 }

	 // Queue up a triple, committing if we have enough chunks
	 private void updateOneTuple( TupleChange tuple )
	 {
		 if( currentLoader == null || !Arrays.equals( currentLoader.getTableDesc().getTables(), tuple.table.getTables() ) ) 
		 {
			 commitTuples() ; // mode is changing, so commit

			 currentLoader = tupleLoaders.get( tuple.table.getTables() ) ;
			 if( currentLoader == null ) 
			 { // make a new loader
				 try 
				 {
					 currentLoader = tupleLoaderClass.getConstructor( String.class, HBaseRdfConnection.class, TableDescLayouts.class, Integer.TYPE ).newInstance( name(), connection(), tuple.table, chunkSize ) ;
				 } 
				 catch( Exception e ) { throw new HBaseRdfException( "Problem making new tupleloader", e ) ;  }
				 currentLoader.start() ;
				 tupleLoaders.put( tuple.table.getTables(), currentLoader ) ;
			 }
		 }
		 if( tuple.toAdd ) currentLoader.load( tuple.tuple ) ;
		 else currentLoader.unload( tuple.tuple ) ;
	 }

	 private void commitTuples() { if ( currentLoader != null ) currentLoader.finish() ; }

	 public void setChunkSize( int chunkSize )            { this.chunkSize = chunkSize ; }

	 public int getChunkSize()                            { return this.chunkSize ; }

	 public void setUseThreading( boolean useThreading )  { this.threading = useThreading ; }

	 public boolean getUseThreading()                     { return this.threading ; }


	 // ---- Bulk loader

	 /**
	  * The (very minimal) thread code
	  */
	 class Commiter implements Runnable
	 {
		 public void run()
		 {
			 log.debug( "Running loader thread" ) ;
			 threadException.set( null ) ;
			 while( true )
			 {
				 try
				 {
					 TupleChange tuple = queue.take() ;
					 if( tuple == flushSignal )
					 {
						 synchronized( threadFlushing ) 
						 {
							 try { commitTuples() ; } catch ( Throwable e ) { handleIssue( e ) ; }
							 threadFlushing.notify() ;
						 }
					 }
					 else if( tuple == finishSignal )
					 {
						 try { commitTuples() ; } catch ( Throwable e ) { handleIssue( e ) ; }
						 break ;
					 }
					 else
						 updateOneTuple( tuple ) ;
				 }
				 catch ( Throwable e ) { handleIssue( e ) ; }
			 }
		 }

		 private void handleIssue( Throwable e ) 
		 {
			 log.error( "Error in thread: " + e.getMessage(), e ) ;
			 threadException.set( e ) ;
		 }
	 }
}