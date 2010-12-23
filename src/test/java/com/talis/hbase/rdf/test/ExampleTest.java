package com.talis.hbase.rdf.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleTest 
{
	@SuppressWarnings("unused")
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();	
	private Configuration configuration = null;

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
	
	@After
	public void tearDown() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(configuration);
		HTableDescriptor[] tables = admin.listTables();
		for (HTableDescriptor hTableDescriptor : tables) {
			admin.disableTable(hTableDescriptor.getName());
			admin.deleteTable(hTableDescriptor.getName());
		}
	}

	@Test
	public void testCreateTable() throws IOException {
		String tableName = "test";
		String[] families = new String[] { "one", "two", "three" };

		HBaseAdmin admin = new HBaseAdmin(configuration);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String family : families) {
			desc.addFamily(new HColumnDescriptor(family));
		}
		admin.createTable(desc);
		HTable table = new HTable(configuration, tableName);
		assertNotNull (table);
		assertTrue(admin.isTableAvailable(tableName));
		
		HTableDescriptor[] tables = admin.listTables();
		assertEquals(1, tables.length);
		assertEquals(tableName, tables[0].getNameAsString());		
	}

	@Test
	public void testPutGet() throws Exception {
		String tableName = "test";
		String[] families = new String[] { "family1", "family2" };
		HBaseAdmin admin = new HBaseAdmin(configuration);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String family : families) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
			hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ); // enhable compression
			desc.addFamily(hColumnDescriptor);
		}
		
		admin.createTable(desc);
		HTable table = new HTable(configuration, tableName);

		Put put = new Put(Bytes.toBytes("rowKey"));
		put.add(Bytes.toBytes("family1"), Bytes.toBytes("qualifier1"), Bytes.toBytes("value1"));
		put.add(Bytes.toBytes("family2"), Bytes.toBytes("qualifier2"), Bytes.toBytes("value2"));
		table.put(put);

		Get get = new Get(Bytes.toBytes("rowKey"));
		Result result = table.get(get);
		assertEquals ("value1", new String(result.getValue(Bytes.toBytes("family1"), Bytes.toBytes("qualifier1"))));
		assertEquals ("value2", new String(result.getValue(Bytes.toBytes("family2"), Bytes.toBytes("qualifier2"))));
		
		ResultScanner scanner = table.getScanner(Bytes.toBytes("family1"));
		Iterator<Result> iter = scanner.iterator();
		int count = 0;
		while ( iter.hasNext() ) {
			iter.next();
			count++;
		}
		assertEquals(1, count);
	}
	
}
