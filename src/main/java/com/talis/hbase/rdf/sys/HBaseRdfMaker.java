package com.talis.hbase.rdf.sys;

import com.hp.hpl.jena.graph.Graph;

public class HBaseRdfMaker 
{
	private static DatasetGraphMakerHBase factory = new DatasetGraphSetup(); ;
	
	public static Graph _createGraph()
    { return factory.createDatasetGraph().getDefaultGraph() ; }
}