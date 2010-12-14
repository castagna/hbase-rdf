package com.talis.hbase.rdf;

public class HBaseRdfFactory {

    public static DatasetGraphHBase createDatasetGraph(LocationHBase location) { 
    	return HBaseRdfMaker._createDatasetGraph(location) ; 
    }
	
}
