package org.archive.io.hdfs;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.archive.io.WriterPoolSettings;
import org.archive.uid.RecordIDGenerator;

/**
 * Settings object for a {@link HDFSWriterPool}.
 * Used creating {@linkHDFSWriter}s.
 */
public class HDFSWriterPoolSettings implements WriterPoolSettings {

    /**
     * Default archival-aggregate filename template.
     *
     * Under usual assumptions -- hostnames aren't shared among crawling hosts;
     * processes have unique PIDs and admin ports; timestamps inside one process
     * don't repeat (see UniqueTimestampService); clocks are generally
     * accurate -- will generate a unique name.
     *
     * Stands for Internet Archive Heritrix.
     */
    public static final String DEFAULT_TEMPLATE =
        "${prefix}-${timestamp17}-${serialno}-${heritrix.pid}~${heritrix.hostname}~${heritrix.port}";

    /** Generator for record IDs */
    private RecordIDGenerator generator;

    /** Template for file names */
    private String template;

    private long maxFileSizeBytes;
    private String prefix;
    private boolean compress;
    private boolean frequentFlushes;
    private int writeBufferSize;
    private List<String> metadata;

    /**
     * Creates a new set of HDFSWriterPoolSettings derived from the
     * provided HDFSParameters.
     *
     * @param parameters Set of HDFSParameters used to populate the
     * new HDFSWriterPoolSettings instance
     */
    public HDFSWriterPoolSettings(long maxFileSizeBytes,
                                  String prefix,
                                  boolean compress,
                                  boolean frequentFlushes,
                                  int writeBufferSize,
                                  List<String> metadata) {

        super();

        this.generator = generator;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.prefix = prefix;
        this.compress = compress;
        this.frequentFlushes = frequentFlushes;
        this.writeBufferSize = writeBufferSize;
        this.metadata = metadata;
    }

    public RecordIDGenerator getRecordIDGenerator() {
        return generator;
    }

    public long getMaxFileSizeBytes() {

        return(maxFileSizeBytes);
    }

    public String getPrefix() {

        return(prefix);
    }

    public String getTemplate() {

        if(template != null) {

            return(template);
        } else {

            return(DEFAULT_TEMPLATE);
        }
    }

    public void setTemplate(String template) {

        this.template = template;
    }

    public boolean getCompress() {

        return(compress);
    }

    public boolean getFrequentFlushes() {

        return(frequentFlushes);
    }

    public int getWriteBufferSize() {

        return(writeBufferSize);
    }

    public List<String> getMetadata() {

        return(metadata);
    }

    public List<File> calcOutputDirs() {

        return(new ArrayList<File>());
    }
}