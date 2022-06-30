import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication
{
	static int r1,c1,r2,c2;
	
	public static class MatrixMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		int i=1,j=1,k=1,l=1,y=1,offset=0;
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			if(key.get()==0)
			{
				String[] dim=value.toString().split(" ");
				r1=Integer.parseInt(dim[0]);
				c1=Integer.parseInt(dim[1]);
				r2=Integer.parseInt(dim[2]);
				c2=Integer.parseInt(dim[3]);
			}
			else if(offset<=r1)
			{
				String[] ele=value.toString().split(" ");
				int x=1;
				
				for(String s:ele)
				{
					while(j<=c2)
					{
						context.write(new Text("("+i+","+j+")"),new Text(x+" "+s));
						j+=1;
					}
					j=1;
					x+=1;
				}
				i+=1;
			}
			else
			{
				String[] ele=value.toString().split(" ");

				for(String s:ele)
				{
					while(k<=r1)
					{
						context.write(new Text("("+k+","+l+")"),new Text(y+" "+s));
						k+=1;
					}
					k=1;
					l+=1;
				}
				l=1;
				y+=1;
			}
			offset+=1;
		}
	}
	
	public static class MatrixReducer extends Reducer<Text,Text,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
		{
			int[] sum=new int[r2];
		
			for(int z=0;z<r2;z++)
				sum[z]=1;
	
			for(Text a:values)
			{
				String[] temp=a.toString().split(" ");
				sum[Integer.parseInt(temp[0])-1]*=Integer.parseInt(temp[1]);
			}

			for(int z=1;z<r2;z++)
				sum[0]+=sum[z];
			context.write(key, new IntWritable(sum[0]));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job=new Job();
		job.setJarByClass(MatrixMultiplication.class);
		FileInputFormat.addInputPath(job, new Path("MatrixInput.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output4"));
		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
