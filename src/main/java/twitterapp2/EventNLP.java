package twitterapp2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.naturalli.NaturalLogicAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

import edu.stanford.nlp.ie.util.RelationTriple;
import edu.stanford.nlp.simple.*;

public class EventNLP {
	static StanfordCoreNLP pipeline = null;
	public static StanfordCoreNLP getOrCreatePipeline ()
	{
		if (pipeline == null)
			{ 
			// set french language  
			pipeline = new StanfordCoreNLP("StanfordCoreNLP-french.properties");
			}
		return pipeline;
	}

  public static void main(String[] args) throws IOException {

  
  	Logger.getLogger("org").setLevel(Level.ERROR);
  	Logger.getLogger("akka").setLevel(Level.ERROR);
  	    
    // Time for reload a new rdd.
    int myTimeWindow = 9;

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterApp");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(myTimeWindow));
    
    
    JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());
    
    // Create stopword blacklist
    Scanner myScanner = new Scanner(new File("datasets/stop_words_french.txt"));
    ArrayList<String> stopwords = new ArrayList<String>();
    while (myScanner.hasNext()){
    	stopwords.add(myScanner.next());
    }
    myScanner.close();
    Broadcast<List<String>> blacklist = jsc.broadcast(stopwords);
    
    // Create a list of hashtags about superbowl.
//    ArrayList<String> superbowls = new ArrayList<String>(
//    		Arrays.asList("#SuperBowl2020", 
//                    "#SuperBowl", "#SuperBowlLIV",
//                    "#SuperBowlnaESPN", "#superbowltf1")); 

    
//    Broadcast<List<String>> superbowlList = jsc.broadcast(superbowls);
    
    // Create a datasets folder in your project . Add the sampleTweets.json in the folder
//    FileReader fr = new FileReader(new File ("datasets/2020-02-04"));
//    BufferedReader br = new BufferedReader(fr);
    String line ;
    int count = 0;
    ArrayList<String> batch = new ArrayList<String>();
    Queue<JavaRDD<String>> rdds = new LinkedList<>();
    
//    while (( line = br.readLine()) != null ) {
//    	count +=1;
//    	if( count == 1000)
//    		{
//    		JavaRDD<String > rdd = jsc.parallelize ( batch );
//    		rdds.add(rdd);
//    		batch = new ArrayList <String >();
//    		count = 0;
//    		}
//    	batch .add( line );
//    }
    
    List<String> filePathList = Arrays.asList("datasets/2020-02-03","datasets/2020-02-04");
    
    for(String filePath : filePathList) {
    	try (BufferedReader br = new BufferedReader(new FileReader(filePath));){
            while (( line = br.readLine()) != null ) {
            	count +=1;
            	if( count == 200)
            		{
            		JavaRDD<String > rdd = jsc.parallelize ( batch );
            		rdds.add(rdd);
            		batch = new ArrayList <String >();
            		count = 0;
            		}
            	batch .add( line );
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    
    JavaDStream <String > stream = jssc.queueStream(rdds , true);
  
    //    JavaDStream<String> infoTweets = stream.map(s ->s.getUser().getName() + " says at "+  s.getCreatedAt().toGMTString() + " the following: "+ s.getText().replace('\n', ' ')); 
    
    // Creating date is a calendar object.
    JavaDStream <Tuple2<String, String>> txtTweets = stream.map (s ->
    {
    Status tweet = TwitterObjectFactory.createStatus(s.toString());
//    Calendar myCal = new GregorianCalendar();
//    myCal.setTime(tweet.getCreatedAt());
//    return new Tuple2<>(tweet.getText().replace('\n', ' '), myCal);
    
    SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHH");
    return new Tuple2<>(tweet.getText().replace('\n', ' '), formater.format(tweet.getCreatedAt()));
    });
 
    
    JavaDStream <Tuple2<String, String>> txtTweetsFiltered = txtTweets.filter(tweet -> {
    	Pattern pattern = Pattern.compile("superbowl", Pattern.CASE_INSENSITIVE);
    	Matcher matcher = pattern.matcher(tweet._1);
    	if(matcher.find()) {
    		return true;
    	} else {
    		return false;
    	}
    });
    

    JavaPairDStream<String, Double> locSent = txtTweetsFiltered.flatMapToPair(s ->
   {
   	   CoreDocument doc = new CoreDocument(s._1); 
   	   getOrCreatePipeline().annotate(doc);
       double overall_sent = 0.0;
//       for (CoreMap sentence : doc.annotation().get(CoreAnnotations.SentencesAnnotation.class)) {
//           Tree tree = sentence.get(SentimentAnnotatedTree.class);
//           int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
//           overall_sent +=sentiment;
//       }
//       overall_sent = overall_sent/doc.sentences().size();
   	ArrayList<Tuple2<String, Double>> mentions = new ArrayList<Tuple2<String, Double>>();
       for (CoreEntityMention em : doc.entityMentions())
       	  mentions.add(new Tuple2<>(em.text() + ", " + em.entityType(), overall_sent));
       return mentions.iterator();
   });; 
   
      
   locSent.foreachRDD( x-> { x.collect().stream().forEach(n-> System.out.println(n._1 + " " + n._2.toString()));});
	   
    
    

    jssc.start();
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
    
    
  }  
}

  

