HDFS (Hadoop Distributed FileSystem) Writer for Heritrix 3.1
============================================================

Contributors:
*  Doug Judd Zvents, Inc. doug at zvents.com
*  Greg Lu OpenPlaces.Org greg.lu at gmail.com
*  Zach Bailey Dataclip.com znbailey at gmail.com
*  Christopher Miles, twitch@nervestaple.com

The heritrix-hadoop-dfs-writer-processor is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HDFS, the Hadoop
Distributed FileSystem (http://lucene.apache.org/hadoop/).  Hadoop implements
the Map/Reduce distributed computation framework on top of HDFS.
heritrix-hadoop-dfs-writer-processor writes crawled content into SequenceFile
format which is directly supported by the Map/Reduce framework and has support
for compression.  This facilitates running high-speed, distributed computations
over content crawled with Heritrix.

This version of the Hadoop HDFS writer was hacked to work with
Heritrix 3.1 and Hadoop version 0.20.205.0 by Christopher Miles. It
might work with newer versions of either Heritrix or Hadoop but, then
again, it may not.

This version of the Hadoop HDFS writer is almost entirely
untested. Miles wanted to do some work with crawl data and Hadoop and
he man-handled this code until it would compile and write out some
data. He hasn't yet checked to see if the data written is at all
intelligible. If you use this code for anything, then extreme caution
is necessary.


Setup
-----

1.  Start hadoop/hdfs
2.  Install heritrix
3.  Untar the current distribution to any directory and once its
untarred enter the <HDFSWriterProcessor_home> directory and run "mvn clean install".
4. Copy the following jar files from your Maven repository
(~/.m2/repository) into the lib/ directory of your Heritrix installation:
   *  heritrix-hadoop-dfs-writer-processor-*.jar
   *  hadoop-*-core.jar
   *  log4j-*.jar
   *  commons-configuration-*.jar
5. Start Heritrix


Configuring Heritrix
--------------------

Heritrix 3 now uses a spring configuration format for configuring the processor beans. This quick section assumes
you're familiar with the basics of Spring and its XML configuration facilities.

First, declare your HDFSParameters bean:

     <bean id="hdfsParameters" class="org.archive.io.hdfs.HDFSParameters">
       <!-- each file written is prefixed with this string (required) -->
       <property name="prefix" value="CrawlData"/>

       <!-- URL for the HDFS name node, not required, default is hdfs://localhost:9000 -->
       <property name="hdfsFsDefaultName" value="hdfs://localhost:9000" />

       <!-- where to write the files to, not required, default is "/crawl" -->
       <property name="hdfsOutputPath" value="/crawl/" />

       <!--
          one of "NONE", "RECORD", "BLOCK", "DEFAULT" (delegates to HDFS sequence file configuration)
          see org.apache.hadoop.io.SequenceFile.CompressionType
       -->
       <property name="hdfsCompressionType" value="NONE" />

     </bean>

Then, declare the processor bean itself:

     <bean id="hdfsWriterProcessor" class="org.archive.modules.writer.HDFSWriterProcessor">
        <property name="hdfsParameters" ref="hdfsParameters"/>
     </bean>

Finally, inject this bean into the DispositionChain:

     <bean id="dispositionProcessors" class="org.archive.modules.DispositionChain">
       <property name="processors">
        <list>
         <ref bean="hdfsWriterProcessor"/>

         ... MORE BEANS HERE ...
        </list>
       </property>
     </bean>


File Format
-----------

Crawl results are stored in the SequenceFile format which is a series of
optionally compressed, key-value documents. Both the key and value of each document
is type org.apache.hadoop.io.Text.

Almost all of the time you're going to read these SequenceFiles as part of a map/reduce job,
so it's strongly recommended that you use the HDFSWriterDocument class to read the
value of each document in your map function like so:

     public void map(Text uri,
                     Text docText,
                     OutputCollector<Text, LongWritable> collector,
                     Reporter reporter) throws IOException {

         HDFSWriterDocument hdfsDoc = new HDFSWriterDocument();
         hdfsDoc.readFields(new DataInputStream(new ByteArrayInputStream(docText.getBytes())));

         //access the document data using hdfsDoc
         //here, we emit the charset and a value of 1 for present:
         String charset = hdfsDoc.getCharset();
         if (charset != null) {
             collector.collect(new Text(charset), new LongWritable(1L));
         }
     }


Advanced Information
--------------------

Note again that HDFSWriterDocument handles reading the data from the low-level format so this may not be that
useful, but we'll document it anyway in case you want to write a parser in another language:

     HDFSWriter/0.3
     <name-value-parameters>
     CRLF
     <http-request> (only for http scheme)
     CRLF
     <http-response-headers> (only for http scheme)
     CRLF
     <response-body>

The following example, illustrates the format:

     HDFSWriter/0.3
     URL: http://www.cnn.com/.element/ssi/www/sect/1.3/misc/contextual/MAIN.html
     Ip-Address: 64.236.29.120
     Crawl-Time: 20070123093916
     Is-Seed: false
     Path-From-Seed: X
     Via: http://www.cnn.com/

     GET /.element/ssi/www/sect/1.3/misc/contextual/MAIN.html HTTP/1.0
     User-Agent: Mozilla/5.0 (compatible; heritrix/1.12.0 +http://www.zvents.com/)
     From: crawler@zvents.com
     Connection: close
     Referer: http://www.cnn.com/
     Host: www.cnn.com
     Cookie: CNNid=46e19fc2-12419-1169545061-167

     HTTP/1.1 200 OK
     Date: Tue, 23 Jan 2007 09:37:46 GMT
     Server: Apache
     Vary: Accept-Encoding,User-Agent
     Cache-Control: max-age=60, private
     Expires: Tue, 23 Jan 2007 09:38:46 GMT
     Content-Length: 3489
     Content-Type: text/html
     Connection: close

     <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
             "http://www.w3.org/TR/html4/loose.dtd">
     <html lang="en">
     <head>
             <meta http-equiv="content-type" content="text/html; charset=iso-8859-1">
             <title>Link Spots CSI</title>
             <script type="text/javascript">
     [...]

The keys of the name-value-parameters can be controlled by setting the relevant
property on the HDFSParameters bean in the spring configuration. For example:

     <bean id="hdfsParameters" class="org.archive.io.hdfs.HDFSParameters">
         ... OTHER PROPERTIES ...

         <!-- default is "Seed-Url", let's change it to "seedUrl" -->
         <property name="seedUrlFieldName" value="seedUrl"/>
     </bean>

The default names can be found by inspecting the HDFSParameters source.


Compiling the Source
--------------------

1.) Fork the git repo
2.) Clone the git repo to your local machine
3.) run "mvn clean install" from the project root


Running an Example Map/Reduce Program
-------------------------------------

The binary and source distributions come with an example map-reduce program
called com.example.mapred.CountCharsets that produces counts
for all of the unique character encodings (charsets) encountered in your
crawled documents.  The source code for this example can be found in the file
src/java/com/example/mapred/CountCharsets.java in the source distribution.

To run this example program, do the following.

1. Copy the heritrix-hadoop-dfs-writer-processor-*.jar file into the lib/
   directory of your hadoop installation.  (NOTE: You should
   push this jar file into the lib directory of all participating
   Hadoop nodes)
2. cd into the hadoop directory
3. Invoke the map-reduce jobs with a line like the following:

     $ ./bin/hadoop com.example.mapred.CountCharsets \
       /heritrix/crawls/no-extract-5-20070130081658484 /output

(Be sure to change the second argument in the above line to where you told Heritrix
to write the crawl data)

This should generate a file in HDFS called /output/part-00000 that contains a
number of lines, one for each unique character set encountered, containing the
name of the character set followed a count.  To see the result, run the
following commands.

     $ ./bin/hadoop fs -text /output/part-00000
