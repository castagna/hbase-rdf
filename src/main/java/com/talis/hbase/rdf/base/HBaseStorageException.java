package com.talis.hbase.rdf.base;

@SuppressWarnings("serial")
public class HBaseStorageException extends RuntimeException
{
    public HBaseStorageException()                          { super() ; }
    public HBaseStorageException(String msg)                { super(msg) ; }
    public HBaseStorageException(Throwable th)              { super(th) ; }
    public HBaseStorageException(String msg, Throwable th)  { super(msg, th) ; }
}