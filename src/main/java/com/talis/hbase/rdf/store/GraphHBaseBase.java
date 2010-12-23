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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Reifier;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.sparql.core.DatasetPrefixStorage;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.graph.GraphBase2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.graph.BulkUpdateHandlerHBase;
import com.talis.hbase.rdf.graph.QueryHandlerHBase;
import com.talis.hbase.rdf.graph.ReifierHBase;
import com.talis.hbase.rdf.graph.TransactionHandlerHBase;
import com.talis.hbase.rdf.graph.UpdateListener;
import com.talis.hbase.rdf.iterator.HBaseSingleRowIterator;
import com.talis.hbase.rdf.iterator.HBaseTableIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

/**
 * A graph implementation for HBase.
 */
public class GraphHBaseBase extends GraphBase2 implements GraphHBase 
{
	private final QueryHandlerHBase queryHandler = new QueryHandlerHBase( this );
	private final TransactionHandler transactionHandler = new TransactionHandlerHBase( this );
	private final BulkUpdateHandler bulkUpdateHandler = new BulkUpdateHandlerHBase( this );
	private final Reifier reifier ;
	protected final DatasetGraphHBase dataset ;
	protected final Node graphNode;

	private final DatasetPrefixStorage prefixes ;
	
	/** The HBase configuration to use **/
	private Configuration config = null;

	/** The three HBase table's for subjects, predicates and objects **/
	private HTable subjects = null, predicates = null, objects = null;

	/** The prefix to use with the table names **/
	private String prefix = null;

	public GraphHBaseBase( DatasetGraphHBase dataset, Node graphName ) 
	{
		super();
		this.dataset = dataset;
		this.graphNode = graphName;
		this.prefixes = new DatasetPrefixesHBase();
		this.reifier = new ReifierHBase( this );
		this.getEventManager().register( new UpdateListener( this ) ) ;
	}

	/**
	 * Constructor
	 * @param prefix - the table prefix name
	 * @param hbaseConfigFile - the configuration file for the HBase master
	 */
	public GraphHBaseBase( String prefix, String hbaseConfigFile, ReificationStyle style ) 
	{
		super();

		//Null for now
		this.dataset = null;
		this.graphNode = null;
		this.prefixes = new DatasetPrefixesHBase();
		this.reifier = new ReifierHBase( this, style );
		
		//Use the deterministic blank node generation algorithm
		JenaParameters.disableBNodeUIDGeneration = true;

		//Initialize a HBase Configuration
		this.config = HBaseConfiguration.create();
		config.addResource( new Path( hbaseConfigFile ) );

		//Set the table prefix
		this.prefix = prefix;

		//Create the three HTable's
		createHTables();
	}

	/**
	 * A method that creates the three HTable's for subjects, predicates and objects.
	 * This method deletes the tables if they already exist.
	 */
	private void createHTables() 
	{
		try
		{
			//Create a HBaseAdmin object
			HBaseAdmin admin = new HBaseAdmin( config );

			//Create the three table names
			String subTableName = prefix + HBaseUtils.SUBJECT_TBL_NAME, predTableName = prefix + HBaseUtils.PREDICATE_TBL_NAME, objTableName = prefix + HBaseUtils.OBJECT_TBL_NAME;

			//If the given tables exist, simply delete them
			if( admin.tableExists( subTableName ) ) { admin.disableTable( subTableName ); admin.deleteTable( subTableName ); }
			if( admin.tableExists( predTableName ) ) { admin.disableTable( predTableName ); admin.deleteTable( predTableName ); }
			if( admin.tableExists( objTableName ) ) { admin.disableTable( objTableName ); admin.deleteTable( objTableName ); }

			//Create the subject HTable
			HTableDescriptor subTableDescriptor = new HTableDescriptor( subTableName );
			admin.createTable( subTableDescriptor );
			admin.disableTable( subTableName );

			HColumnDescriptor subColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME_STR );
			admin.addColumn( subTableName, subColDesc );

			admin.enableTable( subTableName );
			subjects = new HTable( config, subTableName );

			//Create the predicate HTable
			HTableDescriptor predTableDescriptor = new HTableDescriptor( predTableName );
			admin.createTable( predTableDescriptor );
			admin.disableTable( predTableName );

			HColumnDescriptor predColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME_STR );
			admin.addColumn( predTableName, predColDesc );

			admin.enableTable( predTableName );
			predicates = new HTable( config, predTableName );

			//Create the object HTable
			HTableDescriptor objTableDescriptor = new HTableDescriptor( objTableName );
			admin.createTable( objTableDescriptor );
			admin.disableTable( objTableName );

			HColumnDescriptor objColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME_STR );
			admin.addColumn( objTableName, objColDesc );

			admin.enableTable( objTableName );
			objects = new HTable( config, objTableName );
		}
		catch( Exception e ) { throw new JenaException( "Exception in initializing tables: ", e ); }
	}

	/**
	 * @see com.hp.hpl.jena.graph.GraphAdd#add( com.hp.hpl.jena.graph.Triple )
	 */
	@Override
	public void add( Triple t ) 
	{
		try 
		{
			//Get the subject, predicate and object nodes corresponding to the current triple
			Node sub = t.getSubject(), pred = t.getPredicate(), obj = t.getObject();

			//Search and add in the subjects HTable
			Put update = new Put( Bytes.toBytes( sub.toString() ) );
			String val = "";
			Get res = new Get( Bytes.toBytes( sub.toString() ) );
			Result rr = subjects.get( res );
			if( rr.isEmpty() ) { val += pred.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			else { val = Bytes.toString( rr.getValue( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES ) ) + HBaseUtils.CELL_VALUE_SEPARATOR + pred.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			update.add( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES, Bytes.toBytes( val ) );
			subjects.put( update ); subjects.flushCommits(); 
			val = ""; update = null; rr = null; res = null;

			//Search and add in the predicates HTable
			update = new Put( Bytes.toBytes( pred.toString() ) );
			res = new Get( Bytes.toBytes( pred.toString() ) );
			rr = predicates.get( res );
			if( rr.isEmpty() ) { val += sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			else { val = Bytes.toString( rr.getValue( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES ) ) + HBaseUtils.CELL_VALUE_SEPARATOR + sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			update.add( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES, Bytes.toBytes( val ) );
			predicates.put( update ); predicates.flushCommits(); 
			val = ""; update = null; rr = null; res = null;

			//Search and add in the objects HTable
			update = new Put( Bytes.toBytes( obj.toString() ) );
			res = new Get( Bytes.toBytes( obj.toString() ) );
			rr = objects.get( res );
			if( rr.isEmpty() ) { val += sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + pred.toString(); }
			else { val = Bytes.toString( rr.getValue( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES ) )  + HBaseUtils.CELL_VALUE_SEPARATOR + sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + pred.toString(); }
			update.add( HBaseUtils.COL_FAMILY_NAME_BYTES, HBaseUtils.COL_QUALIFIER_NAME_BYTES, Bytes.toBytes( val ) );
			objects.put( update ); objects.flushCommits(); 			
			val = ""; update = null; rr = null;
		}
		catch( Exception e ) { throw new JenaException( "Error in adding new triple: ", e ); }
	}

	/**
	 * @see com.hp.hpl.jena.graph.Graph#find( com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node )
	 */
	@Override
	protected ExtendedIterator<Triple> graphBaseFind( TripleMatch tm )  {
		//Create a Null iterator
		ExtendedIterator<Triple> trIter = NullIterator.instance();
		try {
			//Get the matching subject, predicate and object from the triple
			Node sm = tm.getMatchSubject(), pm = tm.getMatchPredicate(), om = tm.getMatchObject();
			if( sm == null ) sm = Node.ANY; if( pm == null ) pm = Node.ANY; if( om == null ) om = Node.ANY;  

			if( sm.isConcrete() )
			{
				//Get the row corresponding to the subject
				Get res = new Get( Bytes.toBytes( sm.toString() ) );
				Result rr = subjects.get( res );

				//Create an iterator over the triples in that row
				if( !rr.isEmpty() ) trIter = new HBaseSingleRowIterator( rr, sm, pm, om );
			}
			else
				if( om.isConcrete() )
				{
					//Get the row corresponding to the object
					Get res = new Get( Bytes.toBytes( om.toString() ) );
					Result rr = objects.get( res );

					//Create an iterator over the triples in that row
					if( !rr.isEmpty() ) trIter = new HBaseSingleRowIterator( rr, sm, pm, om );
				}
				else
					if( pm.isConcrete() )
					{
						//Get the row corresponding to the predicate
						Get res = new Get( Bytes.toBytes( pm.toString() ) );
						Result rr = objects.get( res );

						//Create an iterator over the triples in that row
						if( !rr.isEmpty() ) trIter = new HBaseSingleRowIterator( rr, sm, pm, om );						
					}
					else
					{
						//Create an iterator over all rows in the subject's HTable
						Scan scanner = new Scan();
						trIter = new HBaseTableIterator( subjects.getScanner( scanner ), sm, pm, om );
					}
		}
		catch( Exception e ) { throw new JenaException( "Error in searching triples: ", e ); }
		return trIter;
	}

	@Override
	public Capabilities getCapabilities() 
	{
		if ( capabilities == null )
			capabilities = new Capabilities() 
			{
				public boolean sizeAccurate() { return true; }
				public boolean addAllowed() { return true ; }
				public boolean addAllowed( boolean every ) { return true; }
				public boolean deleteAllowed() { return true ; }
				public boolean deleteAllowed( boolean every ) { return true; }
				public boolean canBeEmpty() { return true; }
				public boolean iteratorRemoveAllowed() { return false; } /* ** */
				public boolean findContractSafe() { return true; }
				public boolean handlesLiteralTyping() { return false; } /* ** */
			} ;
		return super.getCapabilities() ;
	}

	//@Override
	public final Node getGraphNode() { return graphNode ; }

	//@Override
	public final DatasetGraphHBase getDataset() { return dataset ; }

	//@Override
	public Lock getLock() { return dataset.getLock() ; }

	@Override
	public BulkUpdateHandler getBulkUpdateHandler() { return bulkUpdateHandler ; }

	@Override
	public QueryHandler queryHandler() { return queryHandler ; }

	@Override
	public TransactionHandler getTransactionHandler() { return transactionHandler ; }

	@Override
	protected PrefixMapping createPrefixMapping()  { return prefixes.getPrefixMapping(); }

	public Reifier getReifier() { return reifier; }
	
	@Override
	public void sync() 
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void sync(boolean arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public ReorderTransformation getReorderTransform() {
		// TODO Auto-generated method stub
		return null;
	}
}
