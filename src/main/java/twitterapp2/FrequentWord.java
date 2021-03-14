package twitterapp2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

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

public class FrequentWord {

  public static void main(String[] args) throws IOException {

  
  	Logger.getLogger("org").setLevel(Level.ERROR);
  	Logger.getLogger("akka").setLevel(Level.ERROR);
  	
    // Time for reload a new rdd.
    int myTimeWindow = 1;

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("TwitterApp");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(myTimeWindow));
    
//    String[] filters = {"coronavirus news", "breaking"};        
//    JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
    
    JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());
    
    // Create stopword blacklist
    Scanner myScanner = new Scanner(new File("datasets/stop_words_french.txt"));
    ArrayList<String> stopwords = new ArrayList<String>();
    while (myScanner.hasNext()){
    	stopwords.add(myScanner.next());
    }
    myScanner.close();
    
    // Create a list of customized garbage words.
    ArrayList<String> garbagewords = new ArrayList<String>(
    		Arrays.asList("RT", 
                    "c'est", "C’est",
                    ",", ":", "-", "?", "!", "", "&", "(", ")")); 

    stopwords.addAll(garbagewords);
    
    Broadcast<List<String>> blacklist = jsc.broadcast(stopwords);
    
    // Create a datasets folder in your project . Add the sampleTweets.json in the folder
    FileReader fr = new FileReader(new File ("datasets/2020-02-01"));
    BufferedReader br = new BufferedReader(fr);
    String line ;
    int count = 0;
    ArrayList<String> batch = new ArrayList<String>();
    Queue<JavaRDD<String>> rdds = new LinkedList<>();
    while (( line = br.readLine()) != null ) {
    	count +=1;
    	if( count == 300)
    		{
    		JavaRDD<String > rdd = jsc.parallelize ( batch );
    		rdds.add(rdd);
    		batch = new ArrayList <String >();
    		count = 0;
    		}
    	batch .add( line );
    }
    JavaDStream <String > stream = jssc.queueStream(rdds , true);
  
    //    JavaDStream<String> infoTweets = stream.map(s ->s.getUser().getName() + " says at "+  s.getCreatedAt().toGMTString() + " the following: "+ s.getText().replace('\n', ' ')); 
    
    // Creating date is a calendar object.
    JavaDStream <Tuple2<String, Calendar>> txtTweets = stream.map (s ->
    {
    Status tweet = TwitterObjectFactory.createStatus(s.toString());
    Calendar myCal = new GregorianCalendar();
    myCal.setTime(tweet.getCreatedAt());
    return new Tuple2<>(tweet.getText().replace('\n', ' '), myCal);
    });
 
    
    // select tweets created at night. 
    JavaDStream <Tuple2<String, Calendar>> txtTweetsEvening = txtTweets.filter(tweet -> {
    	if ( 23 < tweet._2.get(Calendar.HOUR_OF_DAY) || tweet._2.get(Calendar.HOUR_OF_DAY) < 5) {
    		return true;
    	} else {
    		return false;
    	}
    });
    
    
    JavaDStream <Tuple2<String, Calendar>> wordsTime = txtTweetsEvening.flatMap(x -> {
    	ArrayList<Tuple2<String, Calendar>> tmp = new ArrayList<Tuple2<String, Calendar>>();
    	for(String mot:Arrays.asList(x._1.split(" "))) {
    		tmp.add(new Tuple2<>(mot,x._2)); 
    	}
    	return tmp.iterator();
    });
    
    // Use blacklist to drop words. Here we ignore case.
    JavaDStream <Tuple2<String, Calendar>> filterdWordsTime = wordsTime.filter(mot -> {
        if (blacklist.value().stream().anyMatch(mot._1::equalsIgnoreCase)) {
            return false;
          } else {
            return true;
          }
        });
    
    // Drop time info 
    JavaPairDStream<String, Integer> filterdWordsTimePair = filterdWordsTime.mapToPair(s -> new Tuple2<>(s._1, 1));
    
    // window operation Reduce last 3*myTimeWindow seconds of data, every 2*myTimeWindow seconds
    JavaPairDStream<String, Integer> filterdWordsTimeOcc = filterdWordsTimePair.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(3 * myTimeWindow), Durations.seconds(2 * myTimeWindow));
 
    JavaPairDStream<Integer, String> filterdWordsTimeOccReverse = filterdWordsTimeOcc.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1));
    JavaPairDStream<Integer, String> sortedFilterdWordsTime = filterdWordsTimeOccReverse.transformToPair(rdd -> rdd.sortByKey(false));        
    
    sortedFilterdWordsTime.foreachRDD( x-> {
        x.collect().stream().limit(10).forEach(n-> System.out.println(n));
    });
    

    jssc.start();
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
    
    
  }  
}

  

