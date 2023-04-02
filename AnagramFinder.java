import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramFinder {
    //hard coded list of stop words to reduce IO overhead. This is defined so early so it can be used across the various classes
    public static List<String> stopWords = Arrays.asList("tis", "twas", "a", "able", "about", "across", "after", "aint", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "arent", "as", "at", "be", "because", "been", "but", "by", "can", "cant", "cannot", "could", "couldve", "couldnt", "dear", "did", "didnt", "do", "does", "doesnt", "dont", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "hasnt", "have", "he", "hed", "hell", "hes", "her", "hers", "him", "his", "how", "howd", "howll", "hows", "however", "i", "id", "ill", "im", "ive", "if", "in", "into", "is", "isnt", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "mightve", "mightnt", "most", "must", "mustve", "mustnt", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "shant", "she", "shed", "shell", "shes", "should", "shouldve", "shouldnt", "since", "so", "some", "than", "that", "thatll", "thats", "the", "their", "them", "then", "there", "theres", "these", "they", "theyd", "theyll", "theyre", "theyve", "this", "tis", "to", "too", "twas", "us", "wants", "was", "wasnt", "we", "wed", "well", "were", "were", "werent", "what", "whatd", "whats", "when", "when", "whend", "whenll", "whens", "where", "whered", "wherell", "wheres", "which", "while", "who", "whod", "wholl", "whos", "whom", "why", "whyd", "whyll", "whys", "will", "with", "wont", "would", "wouldve", "wouldnt", "yet", "you", "youd", "youll", "youre", "youve", "your");

    public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text sortedWord = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //tokenizer takes each word and splits them by the list of characters in the delimiter. It also removes all numbers from the words
            StringTokenizer tokenizer = new StringTokenizer(value.toString().replaceAll("[0-9]", ""), " \t\n\r\f\\,<>|^`~'.:;()#!?-_*$%&=+{}[]@/\"", false);

            while (tokenizer.hasMoreTokens()) {
                //for each word, it trims whitespace from either side of it and makes it lower case
                String token = tokenizer.nextToken().trim().toLowerCase();

                //if the word is longer than one character then it proceeds - this removes boring single character "words"
                if (token.length() > 1){
                    int wordCount = 1;

                    //uses the sorted characters of the word as a key, because if two words are anagrams, their key will be the same, and they will both be values to it
                    sortedWord.set(sort(token));

                    //adds the frequency wordcount to the end of the actual string for easy parsing - this will be removed within the reducer when it needs to be incremented
                    token = token + wordCount;

                    outputValue.set(token);
                    context.write(sortedWord, outputValue);
                }
            }
        }

        //simple function to take a string, sort it's characters into alphabetical order, then return it as a string
        //results from this function are used as a key for the MapReduce
        protected String sort(String input) {
            char[] charSetTemp = input.toCharArray();
            Arrays.sort(charSetTemp);
            return new String(charSetTemp);
        }
    }


    //this combiner class was used before adding the frequency sort functionality.
    //It was used to efficiently remove repeated words and stop words,
    //however we need to count these repeats for frequency sort, so it is commented out
    /*public static class Combiner extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> uniques = new HashSet<Text>();
            for (Text value : values) {
                if (uniques.add(value) &&  ! stopWords.contains(value.toString())) {
                    context.write(key, value);
                }
            }
        }
    }*/



    public static class AnagramReducer extends Reducer<Text,Text,ArrayList<String>,String> {

        //master set that will hold each set of anagrams, this is so that they can be sorted alphabetically later
        private ArrayList<ArrayList<String>> anagramsMasterList = new ArrayList<ArrayList<String>>();

        //this little block of code takes a value as input and finds the index of the first digit
        //this is so we know where to split the value and it's word count
        //this is important for when we have large numbers as they have multiple digits
        protected int findCutOffIndex(String value){
            int cutOffIndex = 0;
            //converts value into char array
            char[] wordCharArr = value.toCharArray();
            //iterates through the char array to find the first digit
            for (int j=0;j<wordCharArr.length;j++){
                if (Character.isDigit(wordCharArr[j])){
                    //if the character is a digit, it returns the index and breaks the for loop
                    cutOffIndex = j;
                    break;
                }
            }
            //returns the index of the first digit
            return cutOffIndex;
        }

        //main reducer function
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //hash set used to check if a word is unique or not so that we know to increment the word count
            Set<String> uniqueWords = new HashSet<String>();
            //the array that will hold the list of anagrams for the current key
            ArrayList<String> anagramWords = new ArrayList<String>();

            //for loop that iterates through all values for the current key
            for (Text value : values) {
                //sends the value to the function to get the index so that it can be split
                int cutOffIndex = findCutOffIndex(value.toString());

                //splits up value into word and wordcount using the above cut off index
                String word = value.toString().substring(0, cutOffIndex);
                int wordCount = Integer.parseInt(value.toString().substring(cutOffIndex));

                //if the word isnt unique, and its not a stop word, just add it to the array of anagrams
                if (uniqueWords.add(word) &&  ! stopWords.contains(word)){
                    anagramWords.add(value.toString());

                }
                //if the word has already been added to the list, increase the word count
                else if (! uniqueWords.add(word) &&  ! stopWords.contains(word)){
                    //search through the list of anagrams for the word we need
                    for (int i=0;i<anagramWords.size();i++){

                        //finding the location of first digit again
                        int cutOffIndex2 = findCutOffIndex(anagramWords.get(i));

                        //fetching just the word
                        String wordFromSet = anagramWords.get(i).substring(0, cutOffIndex2);
                        //checking if the word is the one we want
                        if (wordFromSet.equals(word)){
                            //if it is the correct word, then fetch its wordcount and add it to the other wordcount
                            int wordCountFromSet = Integer.parseInt(anagramWords.get(i).substring(cutOffIndex2));

                            wordCountFromSet = wordCountFromSet + wordCount;

                            //rewrite the word and the updated wordcount into the array at the correct position
                            anagramWords.set(i, wordFromSet + wordCountFromSet);

                            break;
                        }
                    }
                }
            }

            //before the frequency sort, this would of sorted the set of values in alphabetical order
            //Collections.sort(anagramWords);

            //sort words in set by frequency
            //custom comparator to compare each word's wordcount
            Collections.sort(anagramWords, new Comparator<String>() {
                @Override
                public int compare(String word1, String word2) {
                    int cutOffIndex1 = findCutOffIndex(word1);

                    int cutOffIndex2 = findCutOffIndex(word2);

                    int word1Count = Integer.parseInt(word1.substring(cutOffIndex1));
                    int word2Count = Integer.parseInt(word2.substring(cutOffIndex2));

                    //will return a positive or negative value, which will order them accordingly
                    return word2Count - word1Count;
                }
            });

            //once all sorting is done, it adds the set to the master set if there is more than one value
            if (anagramWords.size() > 1) {
                anagramsMasterList.add(anagramWords);
            }
        }

        //cleanup function that sorts the master set of sets by the first item in each subset
        //this results in the master set being in alphabetical order
        public void cleanup(Context context)throws IOException, InterruptedException {
            //sort all sets of anagrams by the first word in each set
            Collections.sort(anagramsMasterList, new Comparator<ArrayList<String>>() {
                @Override
                public int compare(ArrayList<String> arr1, ArrayList<String> arr2) {
                    return arr1.get(0).compareTo(arr2.get(0));
                }
            });

            //finally outputs just the set of values within the master list, no key values - hence the empty string
            for (ArrayList<String> strings : anagramsMasterList) {
                context.write(strings, "");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram Finder");
        job.setJarByClass(AnagramFinder.class);
        job.setMapperClass(AnagramMapper.class);
        //job.setCombinerClass(Combiner.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}