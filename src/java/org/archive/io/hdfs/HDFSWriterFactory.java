package org.archive.io.hdfs;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

public class HDFSWriterFactory extends BasePoolableObjectFactory {

	private final Logger LOG = Logger.getLogger(this.getClass().getName());

	private HDFSParameters _parameters;

	public HDFSWriterFactory(HDFSParameters parameters) {
		_parameters = parameters;
	}

	@Override
	public Object makeObject() throws Exception {
		return new HDFSWriter(_parameters);
	}

	@Override
	public void destroyObject(Object obj) throws Exception {
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
