package org.archive.io.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CrawlController;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.hdfs.HDFSWriterPoolSettings;
import org.archive.util.ArchiveUtils;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * Member of {@link WriterPool}.
 *
 * Implements WriterPoolMember for HDFS files.  Creates a job
 * subdirectory of the same name as the job subdirectory in the
 * Heritrix installation and deposits files there.  It gives some
 * guarantee of uniqueness, and position in file.
 */
public class HDFSWriterPoolMember extends WriterPoolMember implements ArchiveFileConstants, Closeable {

    private final Logger LOGGER = Logger.getLogger(this.getClass().getName());

    /**
     * Reference to file Path object we're currently writing.
     */
    private Path fpath = null;

    /**
     * Reference to file name string since it's used often
     */
    private String fstr = null;

    /**
     * SequenceFile writer
     * This is used to write the output in SequenceFile format
     */
    private SequenceFile.Writer sfWriter = null;

    /**
     * HDFS FileSystem object
     */
    private FileSystem fs = null;
    private final boolean compressed;
    private String prefix = DEFAULT_PREFIX;
    private String suffix = "";
    private final long maxSize;   //pratyush
    private String hdfsOutputPath = null;
    private String hdfsFsDefaultName = "local";
    private String hdfsCompressionType = "DEFAULT";
    private Configuration hdfsConf = null;
    private int hdfsReplication = 3;

    /**
     * Accumulator to hold record contents
     */
    private byte [] accumBuffer = new byte [ 262144 ];
    private int accumOffset = 4;

    /**
     * Creation date for the current file.
     * Set by {@link #createFile()}.
     */
    private String createTimestamp = ArchiveUtils.get14DigitDate();;

    /**
     * NumberFormat instance for formatting serial number.
     *
     * Pads serial number with zeros.
     */
    private static NumberFormat serialNoFormatter = new DecimalFormat("00000");

    public HDFSWriterPoolMember(final AtomicInteger serialNo,
                                HDFSParameters parameters) throws IOException {

        this(serialNo,
             parameters.getPrefix(),
             parameters.isCompression(),
             parameters.getMaxSize(),
             parameters.getHdfsReplication(),
             parameters.getHdfsCompressionType(),
             parameters.getHdfsOutputPath(),
             parameters.getHdfsFsDefaultName(),
             parameters.getSuffix(),
             parameters.getWriterPoolSettings());
    }

    /**
     * Constructor.
     *
     * @param serialNo  used to create unique filename sequences
     * @param jobDir Job directory
     * @param prefix File prefix to use.
     * @param cmprs Compress the records written.
     * @param maxSize Maximum size for ARC files written.
     * @param hdfsReplication Replication factor for HDFS files
     * @param hdfsCompressionType Type of SequenceFile compression to use
     * @param hdfsOutputPath Directory with HDFS where job content files
     *     will get written
     * @param hdfsFsDefaultName fs.default.name Hadoop property
     * @param settings writer pool settings
     * @param suffix suffix for new files
     * @exception IOException
     */
    public HDFSWriterPoolMember(final AtomicInteger serialNo,
                                final String prefix,
                                final boolean cmprs,
                                final long maxSize,
                                final int hdfsReplication,
                                final String hdfsCompressionType,
                                final String hdfsOutputPath,
                                final String hdfsFsDefaultName,
                                final String suffix,
                                HDFSWriterPoolSettings settings)
        throws IOException {

        //super(serialNo, null, prefix, cmprs, maxSize, null);
        super(serialNo, settings, suffix);

        this.prefix = prefix;
        this.suffix = suffix;
        this.maxSize = maxSize;
        this.compressed = cmprs;
        this.hdfsReplication = hdfsReplication;

        if (hdfsOutputPath.endsWith("/"))
            this.hdfsOutputPath =
                hdfsOutputPath.substring(0, hdfsOutputPath.length()-1);
        else
            this.hdfsOutputPath = hdfsOutputPath;

        this.hdfsFsDefaultName = hdfsFsDefaultName;
        this.hdfsCompressionType = hdfsCompressionType;

        this.hdfsConf = new Configuration();
        hdfsConf.set("fs.default.name", this.hdfsFsDefaultName);

        this.fs = FileSystem.get(hdfsConf);

        // make sure the output directory exists
        Path outputDir = new Path(this.hdfsOutputPath);
        fs.mkdirs(outputDir);
    }

    /**
     * Call this method just before/after any significant write.
     *
     * Call at the end of the writing of a record or just before we start
     * writing a new record.  Will close current file and open a new file
     * if file size has passed out maxSize.
     *
     * <p>Creates and opens a file if none already open.
     *
     * @exception IOException
     */
    @Override
    public void checkSize() throws IOException {
        if (sfWriter == null ||
            (this.maxSize != -1 && (this.sfWriter.getLength() > this.maxSize)))
            createFile();
    }

    /**
     * Create a new file.
     *
     * The resulting name looks something like this:
     * IAH-20070111024623-00000-judd.dnsalias.org.open
     * The .gz extension is not included since compression is
     * handled in a different way within HDFS.
     * Usually called from {@link #checkSize()}.
     * @return Name of file created.
     * @throws IOException
     */
    protected String createFile() throws IOException {

        generateNewBasename();
        //TimestampSerialno tsn = getTimestampSerialNo();

        String name = currentBasename + OCCUPIED_SUFFIX;
        //String name = this.prefix + '-' + getUniqueBasename(tsn) + OCCUPIED_SUFFIX;

        close();

        //this.createTimestamp = tsn.getTimestamp();
        fstr  = hdfsOutputPath + "/" + name;
        this.fpath = new Path(fstr);

        // Determine SequenceFile compression type
        SequenceFile.CompressionType compType;

        if (hdfsCompressionType.equals("DEFAULT")) {

            String zname = hdfsConf.get("io.seqfile.compression.type");

            compType = (zname == null) ? SequenceFile.CompressionType.RECORD :
                SequenceFile.CompressionType.valueOf(zname);
        } else {

            compType = SequenceFile.CompressionType.valueOf(hdfsCompressionType);
        }

        int origRep = hdfsConf.getInt("dfs.replication", -1);
        hdfsConf.setInt("dfs.replication", hdfsReplication);

        sfWriter = SequenceFile.createWriter(this.fs,
                                             hdfsConf,
                                             this.fpath,
                                             Text.class,
                                             Text.class,
                                             compType);

        hdfsConf.setInt("dfs.replication", origRep);

        LOGGER.info("Opened " + this.fpath.toString());

        return this.fpath.toString();
    }

    // protected synchronized TimestampSerialno getTimestampSerialNo() {
    //     return getTimestampSerialNo(null);
    // }

    // /**
    //  * Do static synchronization around getting of counter and timestamp so
    //  * no chance of a thread getting in between the getting of timestamp and
    //  * allocation of serial number throwing the two out of alignment.
    //  *
    //  * @param timestamp If non-null, use passed timestamp (must be 14 digit
    //  * ARC format), else if null, timestamp with now.
    //  * @return Instance of data structure that has timestamp and serial no.
    //  */
    // protected synchronized TimestampSerialno getTimestampSerialNo(final String timestamp) {
    //     return new TimestampSerialno((timestamp != null) ? timestamp: ArchiveUtils.get14DigitDate(),
    //                                  serialNo.get());
    // }

    // /**
    //  * Return a unique basename.
    //  *
    //  * Name is timestamp + an every increasing sequence number.
    //  *
    //  * @param tsn Structure with timestamp and serial number.
    //  *
    //  * @return Unique basename.
    //  */
    // private String getUniqueBasename(TimestampSerialno tsn) {
    //     return tsn.getTimestamp() + "-" +
    //         HDFSWriterPoolMember.serialNoFormatter.format(tsn.getSerialNumber());
    // }

    /**
     * Class for serializing an integer to a byte buffer
     */
    private static class IntegerSerializer extends ByteArrayOutputStream {
        DataOutputStream dos = null;

        public IntegerSerializer() {
            super(4);
            dos = new DataOutputStream(this);
        }

        public void write(int val, byte [] dst, int offset) throws IOException {
            reset();
            dos.writeInt(val);

            if (count != 4)
                throw new AssertionError(count != 4);

            System.arraycopy(buf, 0, dst, offset, 4);
        }
    }

    private IntegerSerializer iser = new IntegerSerializer();

    /**
     * Post write tasks.
     *
     * Has side effects.  Will open new file if we're at the upperbound.
     *
     * @exception IOException
     */
    @Override
    protected void preWriteRecordTasks() throws IOException {
        checkSize();
    }

    /**
     * Post file write tasks.
     *
     * @exception IOException
     */
    protected void postWriteRecordTasks(String uri) throws IOException {

        Text key = new Text(uri);
        Text value = new Text();

        iser.write(accumOffset-4, accumBuffer, 0);
        value.set(accumBuffer, 0, accumOffset);
        sfWriter.append(key, value);
        accumOffset = 4;

        if (accumBuffer.length > 1048576)
            accumBuffer = new byte [ 262144 ];

        super.postWriteRecordTasks();
    }

    /**
     * Postion in current physical file.  Used for making accounting
     * of bytes written.
     * @return Position in underlying file.  Call before or after writing
     * records *only* to be safe.
     * @throws IOException
     */
    public long getPosition() {

        long position = 0;

        if (this.sfWriter != null) {

            try {

                // Call flush on underlying file though probably not
                // needed assuming above this.out.flush called through
                // to this.fos.
                sfWriter.syncFs();

                position = this.sfWriter.getLength() + accumOffset;
            } catch(IOException exception) {

                // log a warning, this is not supposed to happen
                LOGGER.warning("Failed to read the length of the current " +
                               "SequenceFile");

                // create a new file to handle our data
                createFile();
            }
        }

        return position;
    }

    public boolean isCompressed() {
        return compressed;
    }

    protected void write(final byte [] b) throws IOException {
        if (accumBuffer.length - accumOffset < b.length)
            growAccumBuffer(b.length-(accumBuffer.length-accumOffset));

        System.arraycopy(b, 0, accumBuffer, accumOffset, b.length);

        accumOffset += b.length;
    }

    protected void write(byte[] b, int off, int len) throws IOException {
        if (accumBuffer.length - accumOffset < len)
            growAccumBuffer(len-(accumBuffer.length-accumOffset));

        System.arraycopy(b, off, accumBuffer, accumOffset, len);

        accumOffset += len;
    }

    protected void write(int b) throws IOException {
        if (accumBuffer.length - accumOffset < 1)
            growAccumBuffer(1);

        accumBuffer[accumOffset] = (byte)b;
        accumOffset++;
    }

    protected void readFullyFrom(final InputStream is, final long recordLength)
        throws IOException {

        int total = 0;
        int remain = accumBuffer.length - accumOffset;

        if (remain < recordLength)
            growAccumBuffer((int)recordLength-remain);

        total = is.read(accumBuffer, accumOffset, (int)recordLength);
        accumOffset += total;

        if (total != recordLength) {
            throw new IOException("Read " + total + " but expected " +
                                  recordLength);
        }
    }

    private void growAccumBuffer(int needed) {
        byte [] newBuf = new byte [ accumBuffer.length + needed + 8192 ];
        System.arraycopy(accumBuffer, 0, newBuf, 0, accumOffset);
        accumBuffer = newBuf;
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing sequence file writer");

        if (this.sfWriter == null) {
            LOGGER.info("Unable to close sequence file writer, it is null.");
            return;
        }

        this.sfWriter.close();
        LOGGER.info("Successfully closed sequence file writer, now renaming file...");

        if (this.fpath != null && this.fs.exists(fpath)) {
            String path = this.fpath.toString();

            if (path.endsWith(OCCUPIED_SUFFIX)) {
                fstr = path.substring(0, path.length() - OCCUPIED_SUFFIX.length());
                Path finalPath = new Path(fstr);

                if (!this.fs.rename(fpath, finalPath)) {
                    LOGGER.warning("Failed rename of " + path);
                }
                LOGGER.info("Successfully renamed " + fstr + " to final path " + finalPath);

                this.fpath = new Path(fstr);
            }

            // not getting size here because it adds more dependency on HDFS
            LOGGER.info("Closed file: " + this.fpath.toString());
        }
    }

    protected String getCreateTimestamp() {
        return createTimestamp;
    }

    public String getFilename() {
        return fstr;
    }
}
