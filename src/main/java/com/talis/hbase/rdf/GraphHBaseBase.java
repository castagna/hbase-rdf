/*
* Copyright © 2010 Talis Systems Ltd.
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

package com.talis.hbase.rdf;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.graph.GraphBase2;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public abstract class GraphHBaseBase  extends GraphBase2 implements GraphHBase {

	private final QueryHandlerHBase queryHandler = new QueryHandlerHBase(this);
	private final TransactionHandler transactionHandler = new TransactionHandlerHBase(this);
	private final BulkUpdateHandler bulkUpdateHandler = new BulkUpdateHandlerHBase(this);
	protected final DatasetGraphHBase dataset ;
	protected final Node graphNode;
	
	public GraphHBaseBase (DatasetGraphHBase dataset, Node graphName) {
		super();
		this.dataset = dataset;
		this.graphNode = graphName;
		this.getEventManager().register(new UpdateListener(this)) ;
	}

    @Override
    public Capabilities getCapabilities()
    {
        if ( capabilities == null )
            capabilities = new Capabilities(){
                public boolean sizeAccurate() { return true; }
                public boolean addAllowed() { return true ; }
                public boolean addAllowed( boolean every ) { return true; } 
                public boolean deleteAllowed() { return true ; }
                public boolean deleteAllowed( boolean every ) { return true; } 
                public boolean canBeEmpty() { return true; }
                public boolean iteratorRemoveAllowed() { return false; } /* ** */
                public boolean findContractSafe() { return true; }
                public boolean handlesLiteralTyping() { return false; } /* ** */
            } ; 
        
        return super.getCapabilities() ;
    }
	
    //@Override
    public final Node getGraphNode() { return graphNode ; }
    
    //@Override
    public final DatasetGraphHBase getDataset()                   { return dataset ; }
    
    //@Override
    public Lock getLock()                                       { return dataset.getLock() ; }
	
    @Override
    public BulkUpdateHandler getBulkUpdateHandler() {return bulkUpdateHandler ; }

    @Override
    public QueryHandler queryHandler()
    { return queryHandler ; }
    
    @Override
    public TransactionHandler getTransactionHandler()
    { return transactionHandler ; }
    
	@Override
	protected PrefixMapping createPrefixMapping() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected ExtendedIterator<Triple> graphBaseFind(TripleMatch m) {
		// TODO Auto-generated method stub
		return null;
	}

}
