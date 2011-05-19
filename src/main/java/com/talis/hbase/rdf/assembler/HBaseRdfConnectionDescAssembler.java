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

package com.talis.hbase.rdf.assembler;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.talis.hbase.rdf.connection.HBaseRdfConnectionDesc;

public class HBaseRdfConnectionDescAssembler extends AssemblerBase implements Assembler
{
    @Override
    public HBaseRdfConnectionDesc open( Assembler a, Resource root, Mode mode )
    {
        HBaseRdfConnectionDesc sDesc = HBaseRdfConnectionDesc.blank() ;
        
        sDesc.setConfig( GraphUtils.getStringValue( root, AssemblerVocab.pHBaseRdfConfiguration ) ) ;
        return sDesc ;
    }
}
