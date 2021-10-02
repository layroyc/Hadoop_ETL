package com.hp.bigdata.page;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hp.bigdata.page.PVStatApp.MyMapper;
import com.hp.bigdata.page.PVStatApp.MyReducer;
import com.hp.bigdata.utils.GetPageId;
import com.hp.bigdata.utils.LogParser;

public class PageStatApp {
	public static void main(String[] args) throws Exception {
		//创建Configuration对象
				Configuration conf = new Configuration();
				//判断输出路径是否存在，存在删除
				FileSystem fs = FileSystem.get(conf);
				Path outputPath = new Path("output");
				if (fs.exists(outputPath)) {
						fs.delete(outputPath, true);
				}
				//创建job对象
				Job job = Job.getInstance(conf);
				//设置提交类
				job.setJarByClass(PageStatApp.class);
				//设置map相关信息
				job.setMapperClass(MyMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongWritable.class);
				//设置reduce相关信息
				job.setReducerClass(MyReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(LongWritable.class);
				//设置输入输出路径
				FileInputFormat.setInputPaths(job, new Path("trackinfo_20130721.txt"));
				FileOutputFormat.setOutputPath(job, new Path("output"));
				//提交
				job.waitForCompletion(true);
	}
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private LogParser parser;
		private LongWritable ONE = new LongWritable(1);
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//实例化
			parser=new LogParser();
		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//读取一行数据
			String line = value.toString();
			//解析url
			Map<String,String> longInfo = parser.parse(line);
			//获取url
			String url = longInfo.get("url");
			//获取pageID
			String pageId = GetPageId.getPageId(url);
			//写入上下文
			context.write(new Text(pageId), ONE);
			
		}
	}
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text pageId, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long sum=0;
			for (LongWritable value : values) {
				sum++;
			}
			context.write(new Text(pageId), new LongWritable(sum));
		}
	}
}
