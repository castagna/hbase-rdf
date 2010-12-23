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

import org.apache.hadoop.conf.Configuration;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.shared.ReificationStyle;
import com.talis.hbase.rdf.sys.HBaseRdfMaker;

public class HBaseRdfFactory {

	public static Model createModel ( Configuration configuration ) { 
    	return ModelFactory.createModelForGraph ( createGraph( configuration ) ) ; 
    }
    
    public static Graph createGraph ( Configuration configuration ) { 
    	return HBaseRdfMaker._createGraph ( configuration ) ; 
    }
    
    public static Graph createGraph ( Configuration configuration, ReificationStyle style ) { 
    	return HBaseRdfMaker._createGraph( configuration, style ) ; 
    }

}