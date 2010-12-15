package edu.utdallas.cs.jena.hbase;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;

/**
 * A model implementation for HBase.
 * @author vaibhav
 *
 */
public class ModelHBase extends ModelCom implements Model
{
	/**
	 * Constructor
	 * @param prefix - the table prefix name
	 * @param hbaseMaster - the url for the HBase master
	 * @param hbaseHomeLoc - the url for the HBase home location
	 */
	public ModelHBase( String prefix, String hbaseMaster, String hbaseHomeLoc )
	{
		this( new GraphHBase( prefix, hbaseMaster, hbaseHomeLoc ) );
	}
	
	/**
	 * Constructor
	 * @param base - the base graph, in this case a HBase graph
	 */
	public ModelHBase( Graph base )  
	{
		super( base );
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
