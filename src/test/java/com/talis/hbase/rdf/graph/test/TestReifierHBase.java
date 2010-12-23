package com.talis.hbase.rdf.graph.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.hp.hpl.jena.db.impl.DBReifier;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Reifier;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.test.GraphTestBase;
import com.hp.hpl.jena.shared.AlreadyReifiedException;
import com.hp.hpl.jena.shared.CannotReifyException;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.util.CollectionFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.talis.hbase.rdf.HBaseRdfFactory;

public class TestReifierHBase extends GraphTestBase
{

	private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();	
	private Configuration configuration = null;
	
	protected static final ReificationStyle Minimal = ReificationStyle.Minimal;
	protected static final ReificationStyle Standard = ReificationStyle.Standard;
	protected static final ReificationStyle Convenient = ReificationStyle.Convenient;
	protected static final Triple ALL = Triple.create( Node.createURI( "??" ), Node.createURI( "??" ), Node.createLiteral( "??" ) );

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TEST_UTIL.startMiniCluster(1);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}
	
	@Before
	public void setUp() {
		configuration = TEST_UTIL.getConfiguration();
	}
	
	public TestReifierHBase( String name )
	{ super( name ); }

	public Graph getGraph() { 
		return HBaseRdfFactory.createGraph ( configuration ) ; 
	}

	public Graph getGraph( ReificationStyle style ) { 
		return HBaseRdfFactory.createGraph ( configuration, style ) ; 
	}

	protected final Graph getGraphWith( String facts )
	{
		Graph result = getGraph();
		graphAdd( result, facts );
		return result;
	}

	protected final Graph graphWithUnless( boolean cond, String facts )
	{ return graphWith( cond ? "" : facts ); }

	protected final Graph graphWithIf( boolean cond, String facts )
	{ return graphWithUnless( !cond, facts ); }

	protected Triple triple( String s, String p, String o )
	{ return Triple.create( Node.createLiteral( s ), Node.createLiteral( p ), Node.createLiteral( o ) ); }

	public void testStyle()
	{
		assertSame( Minimal, getGraph( Minimal ).getReifier().getStyle() );    
		assertSame( Standard, getGraph( Standard ).getReifier().getStyle() );    
		assertSame( Convenient, getGraph( Convenient ).getReifier().getStyle() );    
	}

	public void testEmptyReifiers()
	{
		assertFalse( getGraphWith( "x R y" ).getReifier().findExposed( ALL ).hasNext() );
		assertFalse( getGraphWith( "x R y; p S q" ).getReifier().findExposed( ALL ).hasNext() );
	}

	public void testSameReifier()
	{
		Graph G = getGraph();
		Reifier R1 = G.getReifier();
		G.add( triple( "x R y" ) );
		assertTrue( "same reifier", R1 == G.getReifier() );
	}

	public void testReifierClosed()
	{
		Graph g = getGraph();
		@SuppressWarnings("unused")
		Reifier r = g.getReifier();
		g.close();
	}

	public void testParent()
	{
		Graph G = getGraph(), H = getGraph();
		assertTrue( "correct reifier (G)", G == G.getReifier().getParentGraph() );
		assertTrue( "correct reifier (H)", H == H.getReifier().getParentGraph() );
	}

	public void testStandard()
	{
		Graph g = getGraph( Standard );
		assertFalse( g.getReifier().hasTriple( triple( "s" ,"p", "o" ) ) );
		g.add( Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:subject" ), Node.createLiteral( "s" ) ) );
		assertEquals( 1, g.size() );
		g.add( Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:predicate" ), Node.createLiteral( "p" ) ) );
		assertEquals( 2, g.size() );  
		g.add( Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:object" ), Node.createLiteral( "o" ) ) );
		assertEquals( 3, g.size() );            
		g.add( Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:type" ), Node.createLiteral( "rdf:Statement" ) ) );
		assertEquals( 4, g.size() );
		assertTrue( g.getReifier().hasTriple( triple( "s" ,"p", "o" ) ) );                      
	}

	public void testMinimalExplode()
	{
		Graph g = getGraph( Minimal );
		g.getReifier().reifyAs( node( "a" ), triple( "p Q r" ) );
		assertEquals( 0, g.size() );
	}

	public void testOverspecificationSuppressesReification()
	{
		Graph g = getGraph( Standard );
		Reifier r = g.getReifier();
		graphAdd( g, "x rdf:subject A; x rdf:predicate P; x rdf:object O; x rdf:type rdf:Statement" );
		assertEquals( triple( "A P O" ), r.getTriple( node( "x" ) ) );
		try 
		{ 
			graphAdd( g, "x rdf:subject BOOM" ); 
			assertEquals( null, r.getTriple( node( "x" ) ) ); 
		}
		catch (AlreadyReifiedException e) 
		{
			if (r instanceof DBReifier) {  }
			else throw e;
		}
	}

	public void testReificationSubjectClash()
	{
		testReificationClash( "x rdf:subject SS" );
	}    

	public void testReificationPredicateClash()
	{
		testReificationClash( "x rdf:predicate PP" );
	}    

	public void testReificationObjectClash()
	{
		testReificationClash( "x rdf:object OO" );
	}

	protected void testReificationClash( String clashingStatement )
	{
		Graph g = getGraph( Standard );
		Triple SPO = Triple.create( Node.createURI( "S" ), Node.createURI( "P" ), Node.createLiteral( "O" ) );
		g.getReifier().reifyAs( node( "x" ), SPO );
		assertTrue( g.getReifier().hasTriple( SPO ) );
		try
		{
			graphAdd( g, clashingStatement );
			assertEquals( null, g.getReifier().getTriple( node( "x" ) ) );
			assertFalse( g.getReifier().hasTriple( SPO ) );
		}
		catch (AlreadyReifiedException e)
		{
			if (g.getReifier() instanceof DBReifier) {  }
			else throw e;
		}
	}

	public void testManifestQuadsStandard()
	{ testManifestQuads( Standard ); }

	public void testManifestQuadsConvenient()
	{ testManifestQuads( Convenient ); }

	public void testManifestQuadsMinimal()
	{ testManifestQuads( Minimal ); }

	public void testManifestQuads( ReificationStyle style )
	{
		Graph g = getGraph( style );   
		Reifier r = g.getReifier();
		r.reifyAs( node( "A" ), triple( "S P O" ) );
		String reified = "A rdf:type rdf:Statement; A rdf:subject S; A rdf:predicate P; A rdf:object O";
		assertIsomorphic( graphWithIf( style == Standard, reified ), g );
	}

	public void testRetrieveTriplesByNode()
	{
		Graph G = getGraph();
		Reifier R = G.getReifier();
		Node N = Node.createAnon(), M = Node.createAnon();
		R.reifyAs( N, triple( "x R y" ) );
		assertEquals( "gets correct triple", triple( "x R y" ), R.getTriple( N ) );
		R.reifyAs( M, triple( "p S q" ) );
		assertDiffer( "the anon nodes must be distinct", N, M );
		assertEquals( "gets correct triple", triple( "p S q" ), R.getTriple( M ) );
		assertTrue( "node is known bound", R.hasTriple( M ) );
		assertTrue( "node is known bound", R.hasTriple( N ) );
		assertFalse( "node is known unbound", R.hasTriple( Node.createURI( "any:thing" ) ) );
	}

	public void testRetrieveTriplesByTriple()
	{
		Graph G = getGraph();
		Reifier R = G.getReifier();
		Triple T = triple( "x R y" ), T2 = triple( "y R x" );
		Node N = node( "someNode" );
		R.reifyAs( N, T );
		assertTrue( "R must have T", R.hasTriple( T ) );
		assertFalse( "R must not have T2", R.hasTriple( T2 ) );
	}   

	public void testReifyAs()
	{
		Graph G = getGraph();
		Reifier R = G.getReifier();
		Node X = Node.createURI( "some:uri" );
		assertEquals( "node used", X, R.reifyAs( X, triple( "x R y" ) ) );
		assertEquals( "retrieves correctly", triple( "x R y" ), R.getTriple( X ) );
	}

	public void testAllNodes()
	{
		Reifier R = getGraph().getReifier();
		R.reifyAs( node("x"), triple( "cows eat grass" ) );
		R.reifyAs( node("y"), triple( "pigs can fly" ) );
		R.reifyAs( node("z"), triple( "dogs may bark" ) );
		assertEquals( "", nodeSet( "z y x" ), iteratorToSet( R.allNodes() ) );
	}
	 
	public void testException()
	{
		Graph G = getGraph();
		Reifier R = G.getReifier();
		Node X = node( "x" );
		R.reifyAs( X, triple( "x R y" ) );
		R.reifyAs( X, triple( "x R y" ) );
		try { R.reifyAs( X, triple( "x R z" ) ); fail( "did not detect already reified node" ); }
		catch ( AlreadyReifiedException e ) { }      
	}

	public void testKevinCaseA()
	{
		Graph G = getGraph( Standard );
		Node X = node( "x" ), a = node( "a" ), b = node( "b" ), c = node( "c" );
		G.add( Triple.create( X, RDF.Nodes.type, RDF.Nodes.Statement ) );
		G.getReifier().reifyAs( X, Triple.create( a, b, c ) ); 
	}

	public void testKevinCaseB()
	{
		Graph G = getGraph( Standard );
		Node X = node( "x" ), Y = node( "y" );
		Node a = node( "a" ), b = node( "b" ), c = node( "c" );
		G.add( Triple.create( X, RDF.Nodes.subject, Y ) );
		try
		{
			G.getReifier().reifyAs( X, Triple.create( a, b, c ) );
			fail( "X already has subject Y: cannot make it a" );
		}
		catch ( CannotReifyException e )
		{ pass(); }
	}

	public void testReifierSize()
	{
		Graph g = getGraph();
		Reifier r = g.getReifier();
		assertEquals( 0, r.size() );
	}

	public void testEmpty()
	{
		Graph g = getGraph( Standard );
		assertTrue( g.isEmpty() );
		graphAdd( g, "x rdf:type rdf:Statement" ); assertFalse( g.isEmpty() );
		graphAdd( g, "x rdf:subject Deconstruction" ); assertFalse( g.isEmpty() );
		graphAdd( g, "x rdf:predicate rdfs:subTypeOf" ); assertFalse( g.isEmpty() );
		graphAdd( g, "x rdf:object LiteraryCriticism" ); assertFalse( g.isEmpty() );
	}

	public void testReifierEmptyFind()
	{
		Graph g = getGraph( Standard );
		Reifier r = g.getReifier();
		assertEquals( CollectionFactory.createHashedSet(), iteratorToSet( r.findExposed( triple( "?? ?? ??" ) ) ) );
	}

	public void testReifierFindSubject()
	{ testReifierFind( "x rdf:subject S" ); }

	public void testReifierFindObject()
	{ testReifierFind( "x rdf:object O" ); }

	public void testReifierFindPredicate()
	{ testReifierFind( "x rdf:predicate P" ); }

	public void testReifierFindComplete()
	{ testReifierFind( "x rdf:predicate P; x rdf:subject S; x rdf:object O; x rdf:type rdf:Statement" ); }

	public void testReifierFindFilter()
	{ 
		Graph g = getGraph( Standard );
		Reifier r = g.getReifier();
		graphAdd( g, "s rdf:subject S" );
		assertEquals( tripleSet( "" ), iteratorToSet( r.findExposed( triple( "s otherPredicate S" ) ) ) );
	}

	protected void testReifierFind( String triples )
	{ testReifierFind( triples, "?? ?? ??" ); }

	protected void testReifierFind( String triples, String pattern )
	{
		Graph g = getGraph( Standard );
		Reifier r = g.getReifier();
		graphAdd( g, triples );
		assertEquals(  tripleSet( triples ), iteratorToSet( r.findExposed( triple( pattern ) ) ) );
	}

	public void testHiddenVsReificationMinimal()
	{ testHiddenVsReification( Minimal ); }

	//This test fails, is it supposed to ??
	public void testHiddenVsStandard()
	{ testHiddenVsReification( Standard ); }

	public void testHiddenVsReificationConvenient()
	{ testHiddenVsReification( Convenient ); }

	public void testHiddenVsReification( ReificationStyle style )
	{
		Graph g = getGraph( style );
		Reifier r = g.getReifier();
		r.reifyAs( node( "A" ), triple( "S P O" ) );
		assertEquals( style == Standard, r.findEither( ALL, false ).hasNext() );    
	}

/*	
	//TODO: Dunno why isomorphism does not work, it shows the same triples in actual and expected, nevertheless still throws the error
	public void testStandardExplode()
	{
		Graph g = getGraph( Standard );
		g.getReifier().reifyAs( node( "a" ), triple( "p Q r" ) );
		Graph r = Factory.createDefaultGraph( Minimal );
		graphAdd( r, "a rdf:type rdf:Statement; a rdf:object r; a rdf:predicate Q; a rdf:subject p" );
		assertEquals( 4, g.size() );
		assertIsomorphic( r, g );
	}

	public void testQuintetBug()
	{
		String spec = "rs rdf:type rdf:Statement; foo rdf:value rs; rs rdf:subject X; rs rdf:predicate P; rs rdf:object O1; rs rdf:object O2";
		Graph g = getGraph( Standard );
		Reifier r = g.getReifier();
		try {
			graphAdd( g, spec );
			Graph wanted = getGraph( Minimal );
			graphAdd( wanted, spec );
			assertIsomorphic( wanted, g );
		}
		catch (AlreadyReifiedException e) 
		{
			if (r instanceof DBReifier) { }
			else throw e;
		}
	}
*/
	//TODO: Some bug in here, need to figure it out
/*	public void testReificationTriplesConvenient()
	{ testReificationTriples( Convenient ); }

	public void testReificationTriplesStandard()
	{ testReificationTriples( Standard ); }

	public void testReificationQuadletsMinimal()
	{ testReificationTriples( Minimal ); }

	protected void testReificationTriples( ReificationStyle style )
	{
		Graph g = getGraph( style );
		Graph quadlets = getReificationTriples( g.getReifier() );
		String S1 = "SSS rdf:predicate PPP", S2 = "SSS rdf:subject SSS";
		g.add( triple( S1 ) );
		assertIsomorphic( graphWithUnless( style == Minimal, S1 ), quadlets );
		g.add( triple( S2 ) );
		assertIsomorphic( graphWithUnless( style == Minimal, S1 + "; " + S2 ), quadlets );
		assertEquals( "convenient hides quadlets", style == Convenient, g.size() == 0 );
	}
*/
	//TODO: Remove is not yet added to the graph
/*	public void testRemoveByNode()
	{
		Graph G = getGraph();
		Reifier R = G.getReifier();
		Node X = node( "x" ), Y = node( "y" );
		R.reifyAs( X, triple( "x R a" ) );
		R.reifyAs( Y, triple( "y R a" ) );
		R.remove( X, triple( "x R a" ) );
		assertFalse( "triple X has gone", R.hasTriple( X ) );
		assertEquals( "triple Y still there", triple( "y R a" ), R.getTriple( Y ) );
	}

	public void testRemoveFromNothing()
	{
		Graph G = getGraph();
		@SuppressWarnings("unused")
		Reifier R = G.getReifier();
		G.delete( triple( "quint rdf:subject S" ) );
	}

	public void testQuadRemove()
	{
		Graph g = getGraph( Standard );
		assertEquals( 0, g.size() );

		Triple s = Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:subject" ), Node.createLiteral( "s" ) );
		Triple p = Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:predicate" ), Node.createLiteral( "p" ) );
		Triple o = Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:object" ), Node.createLiteral( "o" ) );
		Triple t = Triple.create( Node.createURI( "x" ), Node.createURI( "rdf:type" ), Node.createLiteral( "rdf:Statement" ) );
		g.add(s); g.add(p); g.add(o); g.add(t);
		assertEquals( 4, g.size() );

		g.delete(s); g.delete(p); g.delete(o); g.delete(t);
		assertEquals( 0, g.size() );
	}
*/

	//TODO: BulkUpdateHandler not implemented
/*	public void testBulkClearReificationTriples()
	{
		Graph g = getGraphWith( "x rdf:subject S" );
		g.getBulkUpdateHandler().removeAll();
		assertEquals( "oops: " + g.getClass(), Collections.EMPTY_SET, g.find( Node.ANY, Node.ANY, Node.ANY ).toSet() );        
	}

	public void testBulkClearReificationTriples2()
	{
		Graph g = getGraphWith( "x rdf:subject S; x rdf:predicate P; x rdf:object O; x rdf:type rdf:Statement" );
		g.getBulkUpdateHandler().removeAll();
		assertEquals( Collections.EMPTY_SET, g.find( Node.ANY, Node.ANY, Node.ANY ).toSet() );        
	}
*/
}
