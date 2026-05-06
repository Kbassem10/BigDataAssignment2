import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ARDriver {

    
    // MAPPER IMPLEMENTATION
    
    public static class ARMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header row
            if (key.get() == 0 || line.startsWith("name,")) return;

            // Simple CSV Split (Handling basic comma separation)
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            // Ensure we have enough columns to prevent ArrayOutOfBounds
            if (fields.length < 8) return;

            try {
                // DATA CLEANING: Extract values and check for null/empty
                String type = fields[5].trim();            // Content Type
                String releaseDate = fields[1].trim();     // released_at
                String genreRaw = fields[2].trim();        // genre
                String ratingRaw = fields[7].trim();       // imdb_rating

                // Validation logic: exclude if any required field is empty[cite: 1]
                if (type.isEmpty() || releaseDate.isEmpty() || genreRaw.isEmpty() || ratingRaw.isEmpty()) {
                    return;
                }

                // EXTRACTION: 1st Genre only[cite: 1]
                // Removes quotes and takes the first element in a comma-separated list
                String firstGenre = genreRaw.replace("\"", "").split(",")[0].trim();

                // EXTRACTION: Release Decade[cite: 1]
                // Takes "2024-05-11" -> "2024" -> 2020 -> "2020s"
                int year = Integer.parseInt(releaseDate.substring(0, 4));
                String decade = (year / 10 * 10) + "s";

                // EXTRACTION: Numeric IMDb rating[cite: 1]
                // Handles "8.5/10" by taking the part before the slash
                String ratingStr = ratingRaw.contains("/") ? ratingRaw.split("/")[0] : ratingRaw;
                double rating = Double.parseDouble(ratingStr.trim());

                // Final safety check for numeric range[cite: 1]
                if (rating < 0 || rating > 10) return;

                // EMIT intermediate data: Key(Type, Genre, Decade) Value(Rating)[cite: 1]
                outKey.set(type + "\t" + firstGenre + "\t" + decade);
                outValue.set(String.valueOf(rating));
                context.write(outKey, outValue);

            } catch (Exception e) {
                // Skips records with invalid dates or non-numeric ratings[cite: 1]
                return;
            }
        }
    }

    
    // BONUS: COMBINER IMPLEMENTATION
    public static class ARCombiner extends Reducer<Text, Text, Text, Text> {
        
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            long count = 0;
            double sum = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (Text val : values) {
                // Combiner aggregates values locally to reduce intermediate data[cite: 1]
                double rating = Double.parseDouble(val.toString());
                sum += rating;
                count++;
                min = Math.min(min, rating);
                max = Math.max(max, rating);
            }

            // Emit a partial aggregate string for the Reducer to finalize[cite: 1]
            // Format: "count,sum,min,max"
            result.set(count + "," + sum + "," + min + "," + max);
            context.write(key, result);
        }
    }
}