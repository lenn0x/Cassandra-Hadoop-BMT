/*
*  Copyright (c) 2009, Chris Goffinet
*
*  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
*
*  The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
*
*  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

 /**
  * Basic Hadoop to Cassandra example
  *
  * Author : Chris Goffinet (goffinet@digg.com)
  */
  
package org.digg;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SelectorManager;
import org.apache.cassandra.service.StorageService;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CassandraBulkLoader {
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
        private Text word = new Text();

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // This is a simple key/value mapper.
            output.collect(key, value);
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Path[] localFiles;
        private ArrayList<String> tokens = new ArrayList<String>();
        private JobConf jobconf;

        public void configure(JobConf job) {
            this.jobconf = job;
            String cassConfig;

            // Get the cached files
            try
            {
                localFiles = DistributedCache.getLocalCacheFiles(job);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            cassConfig = localFiles[0].getParent().toString();

            System.setProperty("storage-config",cassConfig);

            startMessagingService();
            /* 
              Populate tokens 
              
              Specify your tokens and ips below. 
              
              tokens.add("0:192.168.0.1")
              tokens.add("14178431955039102644307275309657008810:192.168.0.2")
            */

            for (String token : this.tokens)
            {
                String[] values = token.split(":");
                StorageService.instance().updateTokenMetadata(new BigIntegerToken(new BigInteger(values[0])),new EndPoint(values[1], 7000));
            }
        }
        public void close()
        {
            try
            {
                // release the cache
                DistributedCache.releaseCache(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), this.jobconf);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
            shutdownMessagingService();
        }
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {
            ColumnFamily columnFamily;
            String Keyspace = "Godspace";
            String CFName = "MyBigAdventureWithBob";
            Message message;
            List<ColumnFamily> columnFamilies;
            columnFamilies = new LinkedList<ColumnFamily>();
            String line;

            /* Create a column family */
            columnFamily = ColumnFamily.create(Keyspace, CFName);
            while (values.hasNext()) {
                // Split the value (line based on your own delimiter)
                line = values.next().toString();
                String[] fields = line.split("\1");
                columnFamily.addColumn(new QueryPath(CFName, fields[1].getBytes("UTF-8"), fields[2].getBytes("UTF-8")),fields[3].getBytes(),0);
            }

            columnFamilies.add(columnFamily);

            /* Get serialized message to send to cluster */
            message = createMessage(Keyspace, key.toString(), CFName, columnFamilies);
            for (EndPoint endpoint: StorageService.instance().getNStorageEndPoint(key.toString()))
            {
                /* Send message to end point */
                MessagingService.getMessagingInstance().sendOneWay(message, endpoint);
            }
            
            output.collect(key, new Text(" inserted into Cassandra node(s)"));

        }
    }

    public static void runJob(String[] args)
    {
        JobConf conf = new JobConf(CassandraBulkLoader.class);

        if(args.length >= 4)
        {
          conf.setNumReduceTasks(new Integer(args[3]));
        }

        try
        {
            // We store the cassandra storage-conf.xml on the HDFS cluster
            DistributedCache.addCacheFile(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), conf);
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setJobName("CassandraBulkLoader_v2");
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        try
        {
            JobClient.runJob(conf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    public static Message createMessage(String Keyspace, String Key, String CFName, List<ColumnFamily> ColumnFamiles)
    {
        ColumnFamily baseColumnFamily;
        DataOutputBuffer bufOut = new org.apache.cassandra.io.DataOutputBuffer();
        RowMutation rm;
        Message message;
        Column column;

        /* Get the first column family from list, this is just to get past validation */
        baseColumnFamily = new ColumnFamily(CFName, "Standard",DatabaseDescriptor.getComparator(Keyspace, CFName), DatabaseDescriptor.getSubComparator(Keyspace, CFName));
        
        for(ColumnFamily cf : ColumnFamiles) {
            bufOut.reset();
            try
            {
                ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
                byte[] data = new byte[bufOut.getLength()];
                System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());

                column = new Column(cf.name().getBytes("UTF-8"), data, 0, false);
                baseColumnFamily.addColumn(column);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        rm = new RowMutation(Keyspace,StorageService.getPartitioner().decorateKey(Key));
        rm.add(baseColumnFamily);

        try
        {
            /* Make message */
            message = rm.makeRowMutationMessage(StorageService.binaryVerbHandler_);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return message;
    }
    public static void startMessagingService()
    {
        SelectorManager.getSelectorManager().start();
    }
    public static void shutdownMessagingService()
    {
        try
        {
            // Sleep just in case the number of keys we send over is small
            Thread.sleep(3*1000);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        // Not implemented in Cassandra trunk, patch forth coming
        MessagingService.flushAndshutdown();
    }
    public static void main(String[] args) throws Exception
    {
        runJob(args);
    }
}