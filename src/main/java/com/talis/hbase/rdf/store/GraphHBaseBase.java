package com.talis.hbase.rdf.store;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.sparql.core.DatasetPrefixStorage;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import com.hp.hpl.jena.sparql.graph.GraphBase2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.talis.hbase.rdf.graph.BulkUpdateHandlerHBase;
import com.talis.hbase.rdf.graph.QueryHandlerHBase;
import com.talis.hbase.rdf.graph.TransactionHandlerHBase;
import com.talis.hbase.rdf.graph.UpdateListener;
import com.talis.hbase.rdf.iterator.HBaseRowIterator;
import com.talis.hbase.rdf.iterator.HBaseTableIterator;
import com.talis.hbase.rdf.util.HBaseUtils;

/**
 * A graph implementation for HBase.
 * @author vaibhav
 *
 */
public class GraphHBaseBase extends GraphBase2 implements GraphHBase
{
	private final QueryHandlerHBase queryHandler = new QueryHandlerHBase(this);
	private final TransactionHandler transactionHandler = new TransactionHandlerHBase(this);
	private final BulkUpdateHandler bulkUpdateHandler = new BulkUpdateHandlerHBase(this);
	protected final DatasetGraphHBase dataset ;
	protected final Node graphNode;

	private final DatasetPrefixStorage prefixes ;
	
	/** The HBase configuration to use **/
	private HBaseConfiguration config = null;

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
		this.getEventManager().register( new UpdateListener( this ) ) ;
	}

	/**
	 * Constructor
	 * @param prefix - the table prefix name
	 * @param hbaseMaster - the url for the HBase master
	 * @param hbaseHomeLoc - the url for the HBase home location
	 */
	public GraphHBaseBase( String prefix, String hbaseMaster, String hbaseHomeLoc )
	{
		super();

		//Null for now
		this.dataset = null;
		this.graphNode = null;
		this.prefixes = new DatasetPrefixesHBase();

		//Use the deterministic blank node generation algorithm
		JenaParameters.disableBNodeUIDGeneration = true;

		//Initialize a HBase Configuration
		this.config = new HBaseConfiguration();
		config.set( "hbase.master" , hbaseMaster );
		config.set( "hbase.rootdir", hbaseHomeLoc );

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
			if( admin.tableExists( subTableName ) )
			{
				//Delete all three tables
				admin.disableTable( subTableName ); admin.deleteTable( subTableName );
				admin.disableTable( predTableName ); admin.deleteTable( predTableName );
				admin.disableTable( objTableName ); admin.deleteTable( objTableName );
			}

			//Create the subject HTable
			HTableDescriptor subTableDescriptor = new HTableDescriptor( subTableName );
			admin.createTable( subTableDescriptor );
			admin.disableTable( subTableName );

			HColumnDescriptor subColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME );
			admin.addColumn( subTableName, subColDesc );

			admin.enableTable( subTableName );
			subjects = new HTable( config, subTableName );

			//Create the predicate HTable
			HTableDescriptor predTableDescriptor = new HTableDescriptor( predTableName );
			admin.createTable( predTableDescriptor );
			admin.disableTable( predTableName );

			HColumnDescriptor predColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME );
			admin.addColumn( predTableName, predColDesc );

			admin.enableTable( predTableName );
			predicates = new HTable( config, predTableName );

			//Create the object HTable
			HTableDescriptor objTableDescriptor = new HTableDescriptor( objTableName );
			admin.createTable( objTableDescriptor );
			admin.disableTable( objTableName );

			HColumnDescriptor objColDesc = new HColumnDescriptor( HBaseUtils.COL_FAMILY_NAME );
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
			BatchUpdate update = new BatchUpdate( Bytes.toBytes( sub.toString() ) );
			String val = "";
			RowResult rr = subjects.getRow( sub.toString() );
			if( rr == null ) { val += pred.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			else { val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + pred.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			subjects.commit( update ); subjects.flushCommits(); 
			val = ""; update = null; rr = null;

			//Search and add in the predicates HTable
			update = new BatchUpdate( Bytes.toBytes( pred.toString() ) );
			rr = predicates.getRow( pred.toString() );
			if( rr == null ) { val += sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			else { val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + obj.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			predicates.commit( update ); predicates.flushCommits(); 
			val = ""; update = null; rr = null;

			//Search and add in the objects HTable
			update = new BatchUpdate( Bytes.toBytes( obj.toString() ) );
			rr = objects.getRow( pred.toString() );
			if( rr == null ) { val += sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + pred.toString(); }
			else { val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + sub.toString() + HBaseUtils.TRIPLE_SEPARATOR + pred.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			objects.commit( update ); objects.flushCommits(); 			
			val = ""; update = null; rr = null;
		}
		catch( Exception e ) { throw new JenaException( "Error in adding new triple: ", e ); }
	}

	/**
	 * @see com.hp.hpl.jena.graph.Graph#find( com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node, com.hp.hpl.jena.graph.Node )
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected ExtendedIterator<Triple> graphBaseFind( TripleMatch tm ) 
	{
		//Create a Null iterator
		ExtendedIterator<Triple> trIter = NullIterator.instance();
		try
		{
			//Get the matching subject, predicate and object from the triple
			Node sm = tm.getMatchSubject(), pm = tm.getMatchPredicate(), om = tm.getMatchObject();
			if( sm == null ) sm = Node.ANY; if( pm == null ) pm = Node.ANY; if( om == null ) om = Node.ANY;  

			if( sm.isConcrete() )
			{
				//Get the row corresponding to the subject
				RowResult rr = subjects.getRow( sm.toString() );

				//Create an iterator over the triples in that row
				trIter = new HBaseRowIterator( rr, sm, pm, om );
			}
			else
				if( om.isConcrete() )
				{
					//Get the row corresponding to the object
					RowResult rr = objects.getRow( om.toString() );

					//Create an iterator over the triples in that row
					trIter = new HBaseRowIterator( rr, sm, pm, om );
				}
				else
					if( pm.isConcrete() )
					{
						//Get the row corresponding to the predicate
						RowResult rr = predicates.getRow( om.toString() );

						//Create an iterator over the triples in that row
						trIter = new HBaseRowIterator( rr, sm, pm, om );						
					}
					else
					{
						//This is for searching over ( ANY @ANY ANY. Construct the columns to be used by the scanner
						String[] columns = new String[1]; columns[0] = HBaseUtils.COL_FAMILY_NAME;

						//Create an iterator over all rows in the subject's HTable
						trIter = new HBaseTableIterator( subjects.getScanner(columns), sm, pm, om );
					}
		}
		catch( Exception e ) { throw new JenaException( "Error in searching triples: ", e ); }
		return trIter;
	}

	@Override
	public Capabilities getCapabilities()
	{
		if ( capabilities == null )
			capabilities = new Capabilities(){
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
	public final DatasetGraphHBase getDataset()                   { return dataset ; }

	//@Override
	public Lock getLock()                                       { return dataset.getLock() ; }

	@Override
	public BulkUpdateHandler getBulkUpdateHandler() {return bulkUpdateHandler ; }

	@Override
	public QueryHandler queryHandler()
	{ return queryHandler ; }

	@Override
	public TransactionHandler getTransactionHandler()
	{ return transactionHandler ; }

	@Override
	protected PrefixMapping createPrefixMapping() 
	{ return prefixes.getPrefixMapping(); }

	@Override
	public void sync() {
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
/*
 * Copyright © 2010 The University of Texas at Dallas
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