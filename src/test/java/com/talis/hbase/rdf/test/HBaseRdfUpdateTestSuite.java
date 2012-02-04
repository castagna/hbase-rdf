/*
 * Copyright © 2010, 2011, 2012 Talis Systems Ltd.
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

package com.talis.hbase.rdf.test;

import junit.framework.TestSuite;

import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.talis.hbase.rdf.test.modify.TestSPARQLUpdate;
import com.talis.hbase.rdf.test.modify.TestSPARQLUpdateMgt;

@RunWith(AllTests.class)
public class HBaseRdfUpdateTestSuite extends TestSuite
{ 
    static public TestSuite suite() { return new HBaseRdfUpdateTestSuite() ; }
    
    private HBaseRdfUpdateTestSuite()
    {
        super("HBase Rdf Update") ;
        
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateSimple.class ) ;
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateVertPart.class ) ;
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateIndexed.class ) ;
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateVPIndexed.class ) ;
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateHybrid.class ) ;
        addTestSuite( TestSPARQLUpdate.TestSPARQLUpdateHash.class ) ;
        
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtSimple.class ) ;
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtVertPart.class ) ;
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtIndexed.class ) ;
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtVPIndexed.class ) ;
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtHybrid.class ) ;
        addTestSuite( TestSPARQLUpdateMgt.TestSPARQLUpdateMgtHash.class ) ;
    }
}
