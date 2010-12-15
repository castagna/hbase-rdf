package com.talis.hbase.rdf.sys;

import com.talis.hbase.rdf.base.table.LocationHBase;
import com.talis.hbase.rdf.store.DatasetGraphHBase;

public class DatasetGraphSetup implements DatasetGraphMakerHBase
{
	@Override
	public DatasetGraphHBase createDatasetGraph() 
	{
		return SetupHBase.buildDataset();
	}

	@Override
	public DatasetGraphHBase createDatasetGraph(LocationHBase location) 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseDatasetGraph(DatasetGraphHBase dataset) 
	{ }

}
