package topten;

import java.util.*;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
	Map<String, String> map = new HashMap<String, String>();
	try {
	    String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
	    for (int i = 0; i < tokens.length - 1; i += 2) {
		String key = tokens[i].trim();
		String val = tokens[i + 1];
		map.put(key.substring(0, key.length() - 1), val);
	    }
	} catch (StringIndexOutOfBoundsException e) {
	    System.err.println(xml);
	}

	return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	// Stores a map of user reputation to the record
	TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
        Map<String, String> parsed = TopTen.transformXmlToMap(value.toString());
            String userId = parsed.get("Id");
            String reputation = parsed.get("Reputation");
            if(userId != null) {
                int rep = Integer.parseInt(reputation);
                repToRecordMap.put(rep, new Text(value));
                    if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
	} catch(Exception e){
        e.printStackTrace();
    }
    }

	protected void cleanup(Context context) throws IOException, InterruptedException {
	    // Output our ten records to the reducers with a null key
	try {
			for(Map.Entry<Integer,Text> en : repToRecordMap.entrySet()) {
                                Text ent= new Text(en.getValue());
				context.write(NullWritable.get(), ent);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	}
    
    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
	// Stores a map of user reputation to the record
	private TreeMap<Integer, Integer> repToRecordMap = new TreeMap<Integer, Integer>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try { 
            for (Text value : values) {
                Map<String, String> parsed = TopTen.transformXmlToMap(value.toString());
                String userId = parsed.get("Id");
                String reputation = parsed.get("Reputation");
		if(userId != null) {
                    int rep = Integer.parseInt(reputation);
                    int userid = Integer.parseInt(userId);
                    repToRecordMap.put(rep, userid);
                    if (repToRecordMap.size() > 10) {
                        repToRecordMap.remove(repToRecordMap.firstKey());
                    }
                }
            }
    
            for (Map.Entry<Integer,Integer> en : repToRecordMap.descendingMap().entrySet()) {
                Put insHBase = new Put(Bytes.toBytes(en.getKey()));
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(en.getValue())); 
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(en.getKey()));
                context.write(null, insHBase);                
            }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }    
    
    public static void main(String[] args) throws Exception {
        Configuration conf =  HBaseConfiguration.create();
        Job job = new Job(conf, "TopTen");        
        job.setJarByClass(TopTen.class);       
        job.setMapperClass(TopTenMapper.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
        job.setReducerClass(TopTenReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
