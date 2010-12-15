package com.talis.hbase.rdf.sys;

import com.talis.hbase.rdf.store.DatasetGraphHBase;

public class SetupHBase 
{
	public static DatasetGraphHBase buildDataset()
	{
		DatasetGraphHBase dsg = new DatasetGraphHBase();
		return dsg;
	}
}
