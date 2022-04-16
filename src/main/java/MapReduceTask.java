import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.net.SyslogAppender;
import java.util.*;
import java.util.Map.Entry;
import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Collectors;

public class MapReduceTask {

//    Map function for counting the appearances of each word in the tweet corpus.
    public static class WordExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
        	
        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
        	
			if(!tweet.equals("")) {
				String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
				/*
				 * Your implementation of the mapper of the word count
				 */
                String t = "";
                for(int i = 0; i < words.length; i++) {
                    if(words[i].isEmpty()) {
                        continue;
                    }
                    context.write(new Text(words[i]), new IntWritable(1));
                }

			}
			//context.write(new Text("missing implementation!"), new IntWritable(1));
        }
    };

//  Reduce function for aggregating the number of appearances of each word in the tweet corpus.
    public static class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        	
        	/*
			 * Your implementation of the reducer of the word count goes here.
			 */
            int word_freq = 0;
            for(IntWritable r: values){
                word_freq += r.get();
            }
            context.write(new Text(key), new IntWritable(word_freq));
        	//context.write(new Text("missing implementation!"), new IntWritable(1));
        }
    }


    /*
     *
     * Here you should write the map, reduce and all other helper methods for
     * solving the other 3 tasks. Observe that the tweet parser and other utility methods
     * are available in TweetParsingUtils.java
     *
     * */

    //    Map function for counting the appearances of each word in the tweet corpus.
    public static class TopWordExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
            String author = parsedCsv.getOrDefault("author", "");
            String tweet = parsedCsv.getOrDefault("tweet", "");

            if(!tweet.equals("")) {
                String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
                /*
                 * Your implementation of the mapper of the word count
                 */
                String t = "";
                for(int i = 0; i < words.length; i++) {
                    if(words[i].isEmpty()) {
                        continue;
                    }
                    context.write(new Text(words[i]), new IntWritable(1));
                }

            }
        }
    };

    //  Reduce function for aggregating the number of appearances of top used word in the tweet corpus.
    public static class TopWordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private String most_freq_word = "";
        private int freq = -1;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            /*
             * Your implementation of the reducer of the word count goes here.
             */

            int word_freq = 0;
            for(IntWritable r: values){
                word_freq += r.get();
            }
            if(word_freq > freq){
                freq = word_freq;
                most_freq_word = key.toString();
            }
            //context.write(new Text(key), new IntWritable(word_freq));
            //context.write(new Text("missing implementation!"), new IntWritable(1));
        }
        @Override
        protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
            context.write(most_freq_word, new IntWritable(freq));
        }
    }



    //    Map function for counting the appearances of each word used by "realDonaldTrump" in the tweet corpus.
    public static class Top10TrumpWordExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
            String author = parsedCsv.getOrDefault("author", "");
            String tweet = parsedCsv.getOrDefault("tweet", "");

            if(author.equals("realDonaldTrump") && !tweet.equals("")) {
                String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
                String t = "";

                for(int i = 0; i < words.length; i++) {
                    if(words[i].isEmpty()) {
                        continue;
                    }
                    context.write(new Text(words[i]), new IntWritable(1));
                }

            }
        }
    };

    //  Reduce function for aggregating the number of appearances of top 10 words used by "realDonaldTrump" in the tweet corpus.
    public static class Top10TrumpWordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String,Integer> map=new HashMap<String,Integer>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            int word_freq = 0;
            for(IntWritable r: values){
                word_freq += r.get();
            }
//            if(word_freq > freq){
//                freq = word_freq;
//                most_freq_word = key.toString();
//            }
            map.put(key.toString(), word_freq);
        }
        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            boolean order = false;
            List<Entry<String, Integer>> list = new LinkedList<>(map.entrySet());

            list.sort((o1, o2) -> order ? o1.getValue().compareTo(o2.getValue()) == 0
                    ? o1.getKey().compareTo(o2.getKey())
                    : o1.getValue().compareTo(o2.getValue()) : o2.getValue().compareTo(o1.getValue()) == 0
                    ? o2.getKey().compareTo(o1.getKey())
                    : o2.getValue().compareTo(o1.getValue()));

            Map<String, Integer> sortedMap = list.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> b, LinkedHashMap::new));

            Iterator itr = sortedMap.entrySet().iterator();

            Integer top10=0;

            while(itr.hasNext()){
                if(top10==10){
                    break;
                }
                Map.Entry mapElement = (Map.Entry)itr.next();
                String a = (String)mapElement.getKey();
                Integer b = (int)mapElement.getValue();

                context.write(new Text(a), new IntWritable(b));
                top10++;
            }
        }
    }




    //    Map function for counting the appearances of each hashTag used by "realDonaldTrump" in the tweet corpus.
    public static class Top10TrumpHashTagExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
            String author = parsedCsv.getOrDefault("author", "");
            String tweet = parsedCsv.getOrDefault("tweet", "");

            if(author.equals("realDonaldTrump") && !tweet.equals("")) {
                String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);

                for(int i = 0; i < words.length; i++) {
//                    char t = words[i].charAt(0);
                    if(words[i].isEmpty() || words[i].charAt(0) != '#') {
                        continue;
                    }
                    context.write(new Text(words[i]), new IntWritable(1));
                }

            }

        }
    };

    //  Reduce function for aggregating the number of appearances of top 10 hash tags of Donald Trump in the tweet corpus.
    public static class Top10TrumpHashTagCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String,Integer> map=new HashMap<String,Integer>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {


            int word_freq = 0;
            for(IntWritable r: values){
                word_freq += r.get();
            }

            map.put(key.toString(), word_freq);
        }
        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            boolean order = false;
            List<Entry<String, Integer>> list = new LinkedList<>(map.entrySet());
            list.sort((o1, o2) -> order ? o1.getValue().compareTo(o2.getValue()) == 0
                    ? o1.getKey().compareTo(o2.getKey())
                    : o1.getValue().compareTo(o2.getValue()) : o2.getValue().compareTo(o1.getValue()) == 0
                    ? o2.getKey().compareTo(o1.getKey())
                    : o2.getValue().compareTo(o1.getValue()));

            Map<String, Integer> sortedMap = list.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue, (a, b) -> b, LinkedHashMap::new));

            Iterator itr = sortedMap.entrySet().iterator();

            Integer top10=0;

            while(itr.hasNext()){
                if(top10==10){
                    break;
                }
                Map.Entry mapElement = (Map.Entry)itr.next();
                String a = (String)mapElement.getKey();
                Integer b = (int)mapElement.getValue();

                context.write(new Text(a), new IntWritable(b));
                top10++;
            }
        }
    }



    /* Method for setting up and executing the word count Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences of each word in the tweets. */
    public void wordCount(String inputWordCount, String outputWordCount) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job wc = Job.getInstance(conf1, "word-count");

        wc.setJarByClass(MapReduceTask.class);
        wc.setMapperClass(WordExtractorMapper.class);
        wc.setReducerClass(WordCounterReducer.class);

        wc.setOutputKeyClass(Text.class);
        wc.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wc, new Path(inputWordCount));
        FileOutputFormat.setOutputPath(wc, new Path(outputWordCount));

        wc.setInputFormatClass(TextInputFormat.class);
        wc.setOutputFormatClass(TextOutputFormat.class);
        wc.waitForCompletion(true);
    }



    /* Method for setting up and executing the top word Hadoop job (most used word in the tweets). This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences of the most used word in the tweets. */
    public void topWord(String inputWordCount, String outputTop1Word) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf2 = new Configuration();
        Job wc1 = Job.getInstance(conf2, "most-count-word");

        wc1.setJarByClass(MapReduceTask.class);
        wc1.setMapperClass(TopWordExtractorMapper.class);
        wc1.setReducerClass(TopWordCounterReducer.class);

        wc1.setOutputKeyClass(Text.class);
        wc1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wc1, new Path(inputWordCount));
        FileOutputFormat.setOutputPath(wc1, new Path(outputTop1Word));

        wc1.setInputFormatClass(TextInputFormat.class);
        wc1.setOutputFormatClass(TextOutputFormat.class);
        wc1.waitForCompletion(true);
    }



    /* Method for setting up and executing the Donald Trump's top 10 words Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used words by Donald Trump. */
    public void top10TrumpWords(String inputWordCount, String outputTop10TrumpWords) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf3 = new Configuration();
        Job wc2 = Job.getInstance(conf3, "top10-trump");

        wc2.setJarByClass(MapReduceTask.class);
        wc2.setMapperClass(Top10TrumpWordExtractorMapper.class);
        wc2.setReducerClass(Top10TrumpWordCounterReducer.class);

        wc2.setOutputKeyClass(Text.class);
        wc2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wc2, new Path(inputWordCount));
        FileOutputFormat.setOutputPath(wc2, new Path(outputTop10TrumpWords));

        wc2.setInputFormatClass(TextInputFormat.class);
        wc2.setOutputFormatClass(TextOutputFormat.class);
        wc2.waitForCompletion(true);
    }



    /* Method for setting up and executing the Donald Trump's top 10 hashtags Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used hashtags by Donald Trump. */
    public void top10TrumpHashtags(String inputWordCount, String outputTop10TrumpHashtags) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf4 = new Configuration();
        Job wc3 = Job.getInstance(conf4, "top10-hashtag-trump");

        wc3.setJarByClass(MapReduceTask.class);
        wc3.setMapperClass(Top10TrumpHashTagExtractorMapper.class);
        wc3.setReducerClass(Top10TrumpHashTagCounterReducer.class);

        wc3.setOutputKeyClass(Text.class);
        wc3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wc3, new Path(inputWordCount));
        FileOutputFormat.setOutputPath(wc3, new Path(outputTop10TrumpHashtags ));

        wc3.setInputFormatClass(TextInputFormat.class);
        wc3.setOutputFormatClass(TextOutputFormat.class);
        wc3.waitForCompletion(true);
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputWordCount = args[0];
        String outputWordCount = args[1];
        String outputTop1Word = args[2];
        String outputTop10TrumpWords = args[3];
        String outputTop10TrumpHashtags = args[4];

        MapReduceTask mrt = new MapReduceTask();

        mrt.wordCount(inputWordCount, outputWordCount);
        mrt.topWord(inputWordCount, outputTop1Word);
        mrt.top10TrumpWords(inputWordCount, outputTop10TrumpWords);
        mrt.top10TrumpHashtags(inputWordCount, outputTop10TrumpHashtags);
    }
}
