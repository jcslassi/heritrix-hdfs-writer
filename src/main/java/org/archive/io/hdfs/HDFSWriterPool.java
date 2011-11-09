package org.archive.io.hdfs;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import org.archive.io.hdfs.HDFSWriterPoolSettings;
import org.archive.io.hdfs.HDFSWriterPoolMember;
import org.archive.io.hdfs.HDFSWriterFactory;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;

/**
 * Provides an object that provides a pool of HDFSWriter instances.
 */
public class HDFSWriterPool extends WriterPool {

    /** Parameters for our HDFSWriter instances */
    private HDFSParameters parameters;

    /** Factory for creating more HDFSWriter instances */
    private HDFSWriterFactory writerFactory;

    /** Logger instance */
    private final Logger logger = Logger.getLogger(this.getClass().getName());

    /**
     * Create a pool of HDFSWriter objects.
     *
     * @param parameters the {@link org.archive.io.hdfs.HDFSParameters} object containing your settings
     * @param poolMaximumActive the maximum number of writers in the writer pool.
     * @param poolMaximumWait the maximum waittime for all writers in the pool.
     */
    public HDFSWriterPool(final AtomicInteger serialNo,
                          final HDFSParameters parameters,
                          final int poolMaximumActive,
                          final int poolMaximumWait) {

        super(serialNo,
              new HDFSWriterPoolSettings(parameters.getMaxSize(),
                                         parameters.getPrefix(),
                                         parameters.isCompression(),
                                         parameters.getFrequentFlushes(),
                                         parameters.getWriteBufferSize(),
                                         parameters.getMetadata()),
              poolMaximumActive,
              poolMaximumWait);

        this.parameters = parameters;
        this.writerFactory = new HDFSWriterFactory(serialNo, parameters);

        // super(
        //       new AtomicInteger(),
        //       new HDFSWriterFactory(parameters),
        //       new HDFSWriterPoolSettings(),
        //       poolMaximumActive,
        //       poolMaximumWait);
    }

    /**
     * Returns another writer for the pool.
     *
     * @return WriterPoolMember
     */
    protected WriterPoolMember makeWriter() {

        WriterPoolMember member = null;

        try {

            member = writerFactory.makeObject();
        } catch(Exception exception) {

            logger.warn("Couldn't create new HDFS writer");
        }

        return(member);
    }
}
