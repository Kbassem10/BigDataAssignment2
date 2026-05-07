import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class ARDriver {

    // 1. MAPPER: Handles Data Extraction and Cleaning
    public static class ARMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (key.get() == 0 || line.startsWith("link,"))
                return;

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            // Ensure we have enough columns (imdb_rating is at index 11)
            if (fields.length < 12)
                return;

            try {
                String releasedAt = fields[3].trim(); // released_at
                String genreRaw = fields[4].trim(); // genre
                String type = fields[9].trim(); // type
                String ratingRaw = fields[11].trim(); // imdb_rating

                if (type.isEmpty() || releasedAt.isEmpty() || genreRaw.isEmpty() || ratingRaw.isEmpty() ||
                        type.equalsIgnoreCase("null") || ratingRaw.equalsIgnoreCase("null")) {
                    return;
                }

                String firstGenre = genreRaw.replace("\"", "").split(",")[0].trim();

                int year = Integer.parseInt(releasedAt.substring(0, 4));
                String decade = (year / 10 * 10) + "s";

                String ratingStr = ratingRaw.contains("/") ? ratingRaw.split("/")[0] : ratingRaw;
                double rating = Double.parseDouble(ratingStr.trim());

                if (rating < 0 || rating > 10)
                    return;

                outKey.set(type + ", " + firstGenre + ", " + decade);
                outValue.set(String.valueOf(rating));
                context.write(outKey, outValue);

            } catch (Exception e) {
                return;
            }
        }
    }

    public static class ARCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (Text val : values) {
                double rating = Double.parseDouble(val.toString());
                sum += rating;
                count++;
                min = Math.min(min, rating);
                max = Math.max(max, rating);
            }
            context.write(key, new Text(count + "," + sum + "," + min + "," + max));
        }
    }

    public static class ARReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalCount = 0;
            double totalSum = 0;
            double globalMin = Double.MAX_VALUE;
            double globalMax = Double.MIN_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                long count = Long.parseLong(parts[0]);
                double sum = Double.parseDouble(parts[1]);
                double min = Double.parseDouble(parts[2]);
                double max = Double.parseDouble(parts[3]);

                totalCount += count;
                totalSum += sum;
                globalMin = Math.min(globalMin, min);
                globalMax = Math.max(globalMax, max);
            }

            double average = totalSum / totalCount;

            String result = String.format("%d, %.2f, %.1f, %.1f", totalCount, average, globalMin, globalMax);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ARDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(ARDriver.class);
        job.setMapperClass(ARMapper.class);
        job.setCombinerClass(ARCombiner.class);
        job.setReducerClass(ARReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}