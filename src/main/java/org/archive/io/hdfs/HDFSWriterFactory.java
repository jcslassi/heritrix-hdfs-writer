package org.archive.io.hdfs;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.archive.io.WriterPoolMember;

public class HDFSWriterFactory extends BasePoolableObjectFactory {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());
    private AtomicInteger serialNo;
    private HDFSParameters parameters;

    public HDFSWriterFactory(AtomicInteger serialNo, HDFSParameters parameters) {

        this.serialNo = serialNo;
        this.parameters = parameters;
    }

    @Override
    public WriterPoolMember makeObject() throws Exception {
        return(new HDFSWriter(serialNo, parameters));
    }

    public void destroyObject(WriterPoolMember obj) throws Exception {
        LOG.info("Asked to destroy object: " + obj);
        try {
            if (obj instanceof Closeable) {
                LOG.info("Found closeable object while destroying. Calling close for " + obj);
                ((Closeable)obj).close();
                LOG.info("Successfully closed object " + obj + " during destroy.");
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }

        super.destroyObject(obj);
    }
}
