HDFS (Hadoop Distributed FileSystem) Writer for Heritrix 3

This file is part of the Heritrix web crawler (crawler.archive.org).

Heritrix is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser Public License as published by
the Free Software Foundation; either version 2.1 of the License, or
any later version.

Heritrix is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser Public License for more details.

You should have received a copy of the GNU Lesser Public License
along with Heritrix; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

Contributors:
Doug Judd Zvents, Inc. doug at zvents.com
Greg Lu OpenPlaces.Org greg.lu at gmail.com
Zach Bailey Dataclip.com znbailey at gmail.com

TABLE OF CONTENTS
=================
* SETUP
* CONFIGURING HERITRIX
* FILE FORMAT
* COMPILING THE SOURCE
* RUNNING AN EXAMPLE MAP-REDUCE PROGRAM
* HELPFUL HINTS

The heritrix-hadoop-dfs-writer-processor is an extension to the Heritrix open
source crawler written by the Internet Archive (http://crawler.archive.org/)
that enables it to store crawled content directly into HDFS, the Hadoop
Distributed FileSystem (http://lucene.apache.org/hadoop/).  Hadoop implements
the Map/Reduce distributed computation framework on top of HDFS.  
heritrix-hadoop-dfs-writer-processor writes crawled content into SequenceFile
format which is directly supported by the Map/Reduce framework and has support
for compression.  This facilitates running high-speed, distributed computations
over content crawled with Heritrix.

This version of heritrix-hadoop-dfs-writer-processor assumes version
3.0 of Heritrix and version 0.20.1+ of Hadoop.  Newer versions of Hadoop
and Heritrix may continue to work with this connector as long as the pertinent
APIs have not changed.  Just replace the jar files with the newer versions.


SETUP
=====

1. Start hadoop/hdfs
2. Install heritrix
3. Untar the current distribution to any directory and once its untarred enter the
   <HDFSWriterProcessor_home> directory and run "ant jar".
4. Copy the following jar files from the heritrix-hadoop-dfs-writer-processor binary
   distribution into the lib/ directory of your Heritrix installation:
	 a) heritrix-hadoop-dfs-writer-processor-*.jar
  	 b) hadoop-*-core.jar
     c) log4j-*.jar
5. Start Heritrix

CONFIGURING HERITRIX
====================

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

FILE FORMAT
===========

The value portion of the SequenceFiles that are generated have the following
format:

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


COMPILING THE SOURCE
====================

1.) Fork the git repo
2.) Clone the git repo to your local machine
3.) run "ant jar" from the project root


RUNNING AN EXAMPLE MAP-REDUCE PROGRAM
=====================================

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

$ ./bin/hadoop org.archive.crawler.examples.mapred.CountCharsets \
      /heritrix/crawls/no-extract-5-20070130081658484 /output

(Be sure to change the second argument in the above line to your Heritrix
 output directory)

This should generate a file in HDFS called /output/part-00000 that contains a
number of lines, one for each unique character set encountered, containing the
name of the character set followed a count.  To see the result, run the
following commands.

$ ./bin/hadoop fs -text /output/part-00000