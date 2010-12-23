package com.talis.hbase.rdf.graph;

import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Reifier;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.shared.AlreadyReifiedException;
import com.hp.hpl.jena.shared.CannotReifyException;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.talis.hbase.rdf.store.GraphHBase;

public class ReifierHBase implements Reifier
{
	private final GraphHBase parent ;
	private ReificationStyle style ;
	private final boolean concealing ;
	
	public ReifierHBase( GraphHBase graph ) { this( graph, ReificationStyle.Standard ); }
	
	public ReifierHBase( GraphHBase graph, ReificationStyle style ) 
	{ this.parent = graph; this.style = style; this.concealing = style.conceals(); }
	
	@Override
	public Triple getTriple( Node n ) 
	{ 
		Node sub = null, pred = null, obj = null ; boolean isStatementFound = false;
		ExtendedIterator<Triple> eit = parent.find( n, Node.ANY, Node.ANY );
		while( eit.hasNext() )
		{
			Triple t = eit.next();
			if( isStatementFound && sub != null && pred != null && obj != null ) { isStatementFound = false; sub = null; pred = null; obj = null; }
			if( t.predicateMatches( RDF.subject.asNode() ) ) sub = t.getObject();
			else if( t.predicateMatches( RDF.predicate.asNode() ) ) pred = t.getObject();
			else if( t.predicateMatches( RDF.object.asNode() ) ) obj = t.getObject();
			else if( t.predicateMatches( RDF.type.asNode() ) && t.objectMatches( RDF.Statement.asNode() ) ) isStatementFound = true;
 		}
		Triple tr = null;
		if( isStatementFound && sub != null && pred != null && obj != null ) tr = Triple.create( sub, pred, obj );
		return tr;
	}

	@Override
	public ExtendedIterator<Node> allNodes() 
	{
		ExtendedIterator<Triple> eit = parent.find( Node.ANY, RDF.type.asNode(), RDF.Statement.asNode() );
		Set<Node> nodes = new HashSet<Node>();
		while( eit.hasNext() ) nodes.add( eit.next().getSubject() );
		return WrappedIterator.create( nodes.iterator() );
	}

	@Override
	public ExtendedIterator<Node> allNodes( Triple t ) 
	{
		Set<Node> nodes = new HashSet<Node>();
		ExtendedIterator<Triple> subEit = parent.find( Node.ANY, RDF.subject.asNode(), t.getSubject() );
		while( subEit.hasNext() ) nodes.add( subEit.next().getSubject() );
		ExtendedIterator<Triple> predEit = parent.find( Node.ANY, RDF.predicate.asNode(), t.getPredicate() );
		Set<Node> predNodes = new HashSet<Node>();
		while( predEit.hasNext() ) predNodes.add( predEit.next().getSubject() );
		ExtendedIterator<Triple> objEit = parent.find( Node.ANY, RDF.object.asNode(), t.getObject() );
		Set<Node> objNodes = new HashSet<Node>();
		while( objEit.hasNext() ) objNodes.add( objEit.next().getSubject() );
		nodes.retainAll( predNodes );
		nodes.retainAll( objNodes );
		return WrappedIterator.create( nodes.iterator() );
	}

	@Override
	public void close() { }

	@Override
	public ExtendedIterator<Triple> find( TripleMatch tm ) 
	{ return matchesReification( tm ) ? parent.find( tm ) : Triple.None ; }

	@Override
	public ExtendedIterator<Triple> findEither( TripleMatch tm, boolean showHidden ) 
	{ return showHidden == concealing ? find( tm ) : Triple.None ; }

	@Override
	public ExtendedIterator<Triple> findExposed( TripleMatch tm ) 
	{ return findEither( tm, false ); }

	@Override
	public Graph getParentGraph() { return parent ; }
	
	@Override
	public ReificationStyle getStyle() { return style ; }

	@Override
	public boolean handledAdd( Triple t ) { return false; }

	@Override
	public boolean handledRemove( Triple t ) { return false; }

	@Override
	public boolean hasTriple( Node n ) 
	{ return getTriple( n ) != null ; }

	@Override
	public boolean hasTriple( Triple t ) 
	{ 
		Triple tr = null;
		ExtendedIterator<Node> iterNodes =  allNodes( t );
		while( iterNodes.hasNext() ) tr = getTriple( iterNodes.next() );
		return tr != null;
	}

	@Override
	public Node reifyAs( Node n, Triple t ) 
	{
		if( parent.find( n, Node.ANY, Node.ANY ).hasNext() ) throw new CannotReifyException( n );
		Triple tr = getTriple( n );
		if( tr != null && tr.equals( t ) ) throw new AlreadyReifiedException( n ) ;
		if( concealing == false ) parentAddQuad( n, t );
		return n;
	}

	private void parentAddQuad( Node n, Triple t )
	{
        parent.add( Triple.create( n, RDF.Nodes.subject, t.getSubject() ) );
        parent.add( Triple.create( n, RDF.Nodes.predicate, t.getPredicate() ) );
        parent.add( Triple.create( n, RDF.Nodes.object, t.getObject() ) );
        parent.add( Triple.create( n, RDF.Nodes.type, RDF.Nodes.Statement ) );		
	}
	
	@Override
	public void remove( Triple t ) 
	{ if( concealing == false ) parent.delete( t ) ; }

	@Override
	public void remove( Node n, Triple t ) 
	{ Triple tr = getTriple( n ); if( tr != null && tr.equals( t ) && concealing == false ) parentRemoveQuad( n, t ) ; }

	private void parentRemoveQuad( Node n, Triple t )
	{
        parent.delete( Triple.create( n, RDF.Nodes.type, RDF.Nodes.Statement ) );
        parent.delete( Triple.create( n, RDF.Nodes.subject, t.getSubject() ) );
        parent.delete( Triple.create( n, RDF.Nodes.predicate, t.getPredicate() ) );
        parent.delete( Triple.create( n, RDF.Nodes.object, t.getObject() ) ); 		
	}
	
	@Override
	public int size() 
	{
		ExtendedIterator<Triple> eit = parent.find( Node.ANY, RDF.type.asNode(), RDF.Statement.asNode() );
		int count = 0;
		while( eit.hasNext() ) { eit.next(); count++; }
		return count * 4;
	}

	private boolean matchesReification( TripleMatch m )
    {
		Node predicate = m.asTriple().getPredicate();
		return 
        !predicate.isConcrete()
        || predicate.equals( RDF.Nodes.subject ) 
        || predicate.equals( RDF.Nodes.predicate ) 
        || predicate.equals( RDF.Nodes.object )
        || predicate.equals( RDF.Nodes.type ) && matchesStatement( m.asTriple().getObject() ) ;
    }
	
	private boolean matchesStatement( Node x )
    { return !x.isConcrete() || x.equals( RDF.Nodes.Statement ); }
}
