package com.talis.hbase.rdf;

import com.hp.hpl.jena.shared.JenaException;

@SuppressWarnings("serial")
public class HBaseRdfException extends JenaException {

    public HBaseRdfException()                          { super() ; }
    public HBaseRdfException(String msg)                { super(msg) ; }
    public HBaseRdfException(Throwable th)              { super(th) ; }
    public HBaseRdfException(String msg, Throwable th)  { super(msg, th) ; }

}