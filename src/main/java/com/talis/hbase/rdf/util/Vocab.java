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

package com.talis.hbase.rdf.util;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class Vocab 
{
    public static Resource type( String namespace, String localName )
    { 
        return ResourceFactory.createResource( namespace + localName ) ;
    }
    
    public static Resource resource( String namespace, String localName )
    {
        return ResourceFactory.createResource( namespace + localName ) ;
    }

    public static Property property( String namespace, String localName )
    {
        return ResourceFactory.createProperty( namespace + localName ) ;
    }
}
