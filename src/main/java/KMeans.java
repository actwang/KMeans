
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.sound.midi.SysexMessage;


public class KMeans {

    // public static boolean isConverged;
    public static final double CONV_THRESHOLD = 0.1;
    public static List<Datapoint> centers = new ArrayList<>(6);
    public static final int k = 6;
    public static List<Boolean> center_converge = new ArrayList<>(6);

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // centroids : Linked-list/arraylist
        // setup will run once for each iteration
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            //System.out.println("Mapper setting up...");

            // retrieve file path
            Path centroids = new Path(conf.get("centroid.path"));   // centroid/cen.seq

            // create a filesystem object
            FileSystem fs = FileSystem.get(conf);

            // create a file reader
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

            // read centroids from the file and store them in a centers variable
            IntWritable key = new IntWritable();
            Text value = new Text();
            //System.out.println("Reading centroids");
            int i = 0;
            // add centroids first, and then update in loop

            while (reader.next(key, value)) {
                // example: value is "1.7885358732482017,19.666057053079573"
                //System.out.println("key = "+ key +", value = "+ value);
                String datapoint_str = value.toString();
                String[] d_str = datapoint_str.split(",");
                //System.out.println("Adding centroid "+ d_str[0]+","+d_str[1]+" to centers arrayList");
                // Update the centroids in the arrayList
                centers.set(i, new Datapoint(Double.parseDouble(d_str[0]), Double.parseDouble(d_str[1])));
                System.out.println("reading line # + " + i);
                i++;
            }
            reader.close();


            //System.out.println("Finished setting up mapper!");
        }

        // mapper will run as many times as there are lines in each iteration
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input -> key: character offset, value -> a point (in Text)
            // write logic to assign a point to a centroid
            // emit key (centroid id(intWritable)) and value (DatapointWritable)
            //System.out.println("Mapping in progress...");
            //Parse the value to a Datapoint
            String dstr_point = value.toString();
            String[] d_str = dstr_point.split(",");
            double d_x = Double.parseDouble(d_str[0]); double d_y = Double.parseDouble(d_str[1]);
            Datapoint d_point = new Datapoint(d_x,d_y);

            // For each centroid in center List, calculate the distance and find the min to assign to
            int min_center_ID = 0; double min_dist = Double.MAX_VALUE;int i = 0;
            //System.out.println(centers.size());
            for (; i < centers.size(); i++){
                Datapoint curr_center = centers.get(i);
                // System.out.println("center" + curr_center);
                double dist = getDistance(curr_center, d_point);
                // System.out.println(dist);
                if (dist < min_dist){
                    min_dist = dist;
                    min_center_ID = i;
                }
            }

            //System.out.println("min distance = "+min_dist);

            // Now min_center_ID represents the center that the datapoint belongs to after looping
            // System.out.println("centers size = "+ centers.size());


            //System.out.println("Mapping Done! Center assigned to point "+ dstr_point+" is center #"+
            //        min_center_ID+ " with coordinates "+str_x+","+str_y+"\n\n");
            context.write(new IntWritable(min_center_ID), new Text(d_x + "," + d_y));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Input: key -> centroid id , value -> list of new centers
            //          1           [(3,4), (3,9)] —> 3,6.5
            // calculate the new centroid
            // new_centroids.add() (store updated centroid in a variable)
            //System.out.println("Reducing in progress...");
            /* for all points in each key value pair, add them up (x,y), divide by total to get mean and that's the new center*/
            double sum_x = 0; double sum_y = 0; int num_points = 0;
            for (Text val: values){
                //convert val from Text to double
                double val_x = Double.parseDouble(val.toString().split(",")[0]);
                double val_y = Double.parseDouble(val.toString().split(",")[1]);
                sum_x += val_x; sum_y += val_y;
                num_points += 1;
            }
            double newCenter_x = sum_x / num_points;
            double newCenter_y = sum_y / num_points;
            //System.out.println("number of points = "+ num_points);

            // Calculate the distance between old and new center
            //int center_idx = Integer.parseInt(key.toString());
            int center_idx = key.get();

            Datapoint currCenter = centers.get(center_idx);
            Datapoint newCenter = new Datapoint(newCenter_x, newCenter_y);
            double center_dist = getDistance(newCenter, currCenter);
            System.out.println("Old center ID = "+center_idx +" : "+ currCenter+ " new Center at "+newCenter);
            // System.out.println("Distance for center # "+ center_idx + " is "+ center_dist);

            // if distance is less than threshold, it converged
            if (center_dist < CONV_THRESHOLD){
                center_converge.set(center_idx, true);
                System.out.println("Converged center # "+ center_idx);
            }else{
                center_converge.set(center_idx, false);
            }
            // Update the center to the new computed values
            currCenter.setXY(newCenter_x, newCenter_y);

            // datapoint output
            // System.out.println("Reducer Done!\n\n");
            context.write(NullWritable.get(), new Text(newCenter_x +","+ newCenter_y));
        }

        //called once for each split
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // BufferedWriter
            // delete the old centroids
            // write the new centroids aka update centroids, done in reducer
            //update the centroids written in hdfs://localhost:9000/A1/cen.seq
            Configuration conf = context.getConfiguration();
            Path centerPath = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(centerPath)){
                // System.out.println("Centroid.path deleted!!!");
                fs.delete(centerPath,true);
            }
            final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, centerPath, IntWritable.class,
                    Text.class);
            final IntWritable value = new IntWritable(0);

            for (Datapoint centroid :centers){
                centerWriter.append(value, new Text(centroid.getX() +","+ centroid.getY()));
            }

            //System.out.println("New centroid: ("+ newCenter_x +","+newCenter_y+")");
            centerWriter.close();
        }

        public static enum Counter {
            CONVERGED
        }
        // new_centroids (variable to store the new centroids)
    }

    public static void mapReduce(Configuration conf, String inputHDFSPath, String outputHDFSPath) throws Exception {
        // configuration
        Path outpath = new Path("hdfs://localhost:9000/"+outputHDFSPath);
        FileSystem fs = (outpath).getFileSystem(conf);
        if (fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputHDFSPath));
        FileOutputFormat.setOutputPath(job, new Path(outputHDFSPath));
        job.waitForCompletion(true);
    }

    public static double getDistance(Datapoint p1, Datapoint p2){
        return Math.sqrt(Math.pow(p1.getX() - p2.getX(), 2) + Math.pow(p1.getY() - p2.getY(), 2));
    }

    public static boolean notConverged(){
        if (null==center_converge){
            System.out.println("NULLLLL");
            return true;
        }
        // Check all the center_converges, return true if one or more is not converged
        for (boolean bool: center_converge){
            if (!bool){
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String hdfs_addr = "hdfs://localhost:9000/";

        Path center_path = new Path("A1/cen.seq");
        conf.set("centroid.path", center_path.toString());
        conf.set("fs.defaultFS", hdfs_addr);

        FileSystem fs = FileSystem.get(conf);
        // if cen.seq already created, delete it
        if (fs.exists(center_path)) {
            System.out.println("cen.seq already exists, deleting it for new current program.");
            fs.delete(center_path, true);
        }

        final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, IntWritable.class,
                Text.class);
        final IntWritable value = new IntWritable(0);

        // Write random center points to A1/cen.seq before any iteration starts
        centerWriter.append(value, new Text("50.197031637442876,32.94048164287042"));
        centerWriter.append(value, new Text("43.407412339767056,6.541037020010927"));
        centerWriter.append(value, new Text("1.7885358732482017,19.666057053079573"));
        centerWriter.append(value, new Text("32.6358540480337,4.03843047564191"));
        centerWriter.append(value, new Text("22.6358540480337,8.2388923768291"));
        centerWriter.append(value, new Text("2.7872684420337,20.898430475691"));

        centerWriter.close();
        // upload data_points.txt to HDFS
        uploadToHDFS("/Users/zi/Desktop/22Fall/MIE1628/Assignments/A1_Zicong_Wang/Code/KMeans" +
                "/data_points.txt", hdfs_addr+"A1/input", conf);

        // Add the center points to the centers arrayList
        centers.add(0,new Datapoint(5.197031637442876,42.94048164287042));
        centers.add(1,new Datapoint(4.407412339767056,69.541037020010927));
        centers.add(2,new Datapoint(1.7885358732482017,9.666057053079573));
        centers.add(3,new Datapoint(32.6358540480337,4.03843047564191));
        centers.add(4,new Datapoint(22.6358540480337,8.2388923768291));
        centers.add(5,new Datapoint(2.7872684420337,20.898430475691));

        for (int i = 0; i < k; i++){
            center_converge.add(false);
        }
        long start_time = System.currentTimeMillis();
        int itr = 0;
        while (itr < 10 && notConverged()) {
            System.out.println("centers size = "+ centers.size());

            System.out.println("Starting iteration " + itr + "......");
            mapReduce(conf, "hdfs://localhost:9000/A1/input","/A1/output");
            itr+=1;
        }

        // After everything is done, read the final centroid file from hdfs and print the centroids
        copyToLocal(hdfs_addr + "A1/output" + "/part-r-00000", "./output6.txt", conf);
        Scanner scanner = new Scanner(new File("./output6.txt"));
        while (scanner.hasNextLine()){
            String Line = scanner.nextLine();
            System.out.println(Line);
        }

        long end_time = System.currentTimeMillis();
        System.out.println("Time passed = "+(end_time - start_time) / 1000 + "s");
        System.out.println("Itr = "+ itr);
    }

    public static void copyToLocal(String hdfsPath, String localPath, Configuration conf) throws IOException {
        // Copy the output file from hdfs to local output.txt
        Path hdfs_path = new Path(hdfsPath);
        FileSystem fs = hdfs_path.getFileSystem(conf);
        fs.copyToLocalFile(hdfs_path, new Path(localPath));
    }

    public static void uploadToHDFS(String localfilePath, String hdfsFilePath,Configuration conf) throws IOException {
        Path localPath = new Path(localfilePath);
        Path hdfsPath = new Path(hdfsFilePath);
        System.out.println(hdfsFilePath);

        FileSystem fs = hdfsPath.getFileSystem(conf);
        if (!fs.exists(hdfsPath)){
            // if path doesn't exist, create it
            System.out.println("HDFS input path for datapoint.txt doesn't exist, creating it right now...");
            fs.mkdirs(hdfsPath);
        }
        fs.copyFromLocalFile(localPath,hdfsPath); //copy local, paste to HDFS
        if (fs.exists(new Path(hdfsFilePath + "/data_points.txt"))){
            System.out.println("data_points.txt uploaded susccessfully!");
        }
    }
}