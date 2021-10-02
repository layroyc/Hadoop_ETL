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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.hp.bigdata.utils.GetPageId;
import com.hp.bigdata.utils.LogParser;

public class ETLApp {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path("input");
		if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
		}
		//创建job对象
		Job job = Job.getInstance(conf);
		//设置提交类
		job.setJarByClass(ETLApp.class);
		//设置map相关信息
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path("trackinfo_20130721.txt"));
		FileOutputFormat.setOutputPath(job, new Path("input"));
		//提交
		job.waitForCompletion(true);
	}
	static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		//初始化
		private LogParser parser;
		//重写setup方法
		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			parser=new LogParser();
		}
		//重写map方法
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			Map<String,String> longInfo = parser.parse(line);
			//获取一行有效字段
			String ip = longInfo.get("ip");
			String url = longInfo.get("url");
			String sessionId = longInfo.get("sessionId");
			String time = longInfo.get("time");
			String country = longInfo.get("country") == null ? "-" : longInfo.get("country");
			String province = longInfo.get("province") == null ? "-" : longInfo.get("province");
			String city = longInfo.get("city") == null ? "-" : longInfo.get("city");
			String pageId = GetPageId.getPageId(url) == null ? "-" : GetPageId.getPageId(url);
			
			//创建缓冲区
			StringBuilder builder = new StringBuilder();
			builder.append(ip+"\t");
			builder.append(url+"\t");
			builder.append(sessionId+"\t");
			builder.append(time+"\t");
			builder.append(country+"\t");
			builder.append(province+"\t");
			builder.append(city+"\t");
			builder.append(pageId+"\t");
			
			//写入上写文
			context.write(NullWritable.get(), new Text(builder.toString()));
			
			}
	}
}
