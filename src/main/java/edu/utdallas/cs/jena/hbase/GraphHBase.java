package edu.utdallas.cs.jena.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

import edu.utdallas.cs.jena.hbase.util.HBaseUtils;

public class GraphHBase extends GraphBase implements Graph
{
	/** The HBase configuration to use **/
	private HBaseConfiguration config = null;
	
	/** The three HBase table's for subjects, predicates and objects **/
	private HTable subjects = null, predicates = null, objects = null;
	
	/** The prefix to use with the table names **/
	private String prefix = null;
	
	public GraphHBase( String prefix, String hbaseHomeLoc )
	{
		JenaParameters.disableBNodeUIDGeneration = true;
		this.config = new HBaseConfiguration();
		config.addResource( hbaseHomeLoc + "conf/hbase-site.xml" );
		this.prefix = prefix;
		createHTables();
	}

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
	
	public void add( Triple t )
	{
		try
		{
			Node sub = t.getSubject(), pred = t.getPredicate(), obj = t.getObject();
			
			//Search and add in the subjects HTable
			BatchUpdate update = new BatchUpdate( Bytes.toBytes( sub.toString() ) );
			String val = "";
			RowResult rr = subjects.getRow( sub.toString() );
			if( rr == null ) { val += t.toString(); }
			else 
			{ val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + t.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			subjects.commit( update ); subjects.flushCommits(); 
			val = ""; update = null; rr = null;
			
			//Search and add in the predicates HTable
			update = new BatchUpdate( Bytes.toBytes( pred.toString() ) );
			rr = predicates.getRow( pred.toString() );
			if( rr == null ) { val += t.toString(); }
			{ val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + t.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			predicates.commit( update ); predicates.flushCommits(); 
			val = ""; update = null; rr = null;

			//Search and add in the objects HTable
			update = new BatchUpdate( Bytes.toBytes( obj.toString() ) );
			rr = objects.getRow( pred.toString() );
			if( rr == null ) { val += t.toString(); }
			{ val = Bytes.toString( rr.get( Bytes.toBytes( HBaseUtils.COL_FAMILY_NAME ) ).getValue() ) + HBaseUtils.CELL_VALUE_SEPARATOR + t.toString(); }
			update.put( HBaseUtils.COL_FAMILY_NAME, Bytes.toBytes( val ) );
			objects.commit( update ); objects.flushCommits(); 			
			val = ""; update = null; rr = null;
		}
		catch( Exception e ) { throw new JenaException( "Error in adding new triple: ", e ); }
	}
	
	@Override
	protected ExtendedIterator<Triple> graphBaseFind( TripleMatch tm ) 
	{
		try
		{
			Node sm = tm.getMatchSubject(), pm = tm.getMatchPredicate(), om = tm.getMatchObject();
			if( sm == null ) sm = Node.ANY; if( pm == null ) pm = Node.ANY; if( om == null ) om = Node.ANY;
			
			if( sm.isConcrete() )
			{
				RowResult rr = subjects.getRow( sm.toString() );
			}
		}
		catch( Exception e ) { throw new JenaException( "Error in searching triples: ", e ); }
		return null;
	}
}
