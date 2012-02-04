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

import com.talis.hbase.rdf.test.query.TestQueryHBaseRdfFactory;

@RunWith(AllTests.class)
public class HBaseRdfQueryTestSuite extends TestSuite
{
    //  static suite() becomes in JUnit 4:... 
    //  @RunWith(Suite.class) and SuiteClasses(TestClass1.class, ...)
    
    // @RunWith(Parameterized.class) and parameters are sdb files or Stores
    // But does not allow for programmatic construction of a test suite.

    // Old style (JUnit3) but it allows programmatic
    // construction of the test suite hierarchy from a script.
    static public TestSuite suite() { return new HBaseRdfQueryTestSuite() ; }
    
    private HBaseRdfQueryTestSuite()
    {
        super( "HBaseRdf" ) ;
        
        TestQueryHBaseRdfFactory.make( this, HBaseRdfTestSetup.storeList, HBaseRdfTestSetup.manifestMain ) ;
    }
}
