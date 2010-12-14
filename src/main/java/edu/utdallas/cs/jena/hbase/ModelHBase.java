package edu.utdallas.cs.jena.hbase;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;

public class ModelHBase extends ModelCom implements Model
{
	public ModelHBase( String prefix, String hbaseHomeLoc )
	{
		this( new GraphHBase( prefix, hbaseHomeLoc ) );
	}
	
	public ModelHBase( Graph base )  
	{
		super( base );
	}
}
