
import java.util.*;
import java.util.concurrent.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class TweetSentimentProcessor {
	private static final String TweetQueueName = "TweetQueue";
	
	private final int ThreadPoolSize = 10;
	private AmazonSQS sqs;
	private String queueUrl;
	private ExecutorService executor = Executors.newFixedThreadPool(ThreadPoolSize);
	
	public TweetSentimentProcessor() {
		// Get the credentials
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Could not load credential profiles", e);
        }
        
        // Initialize SQS for USEast
        sqs = new AmazonSQSClient(credentials);
        Region usEast = Region.getRegion(Regions.US_EAST_1);
        sqs.setRegion(usEast);

        // Get the TweetQueue URL
        queueUrl = sqs.getQueueUrl(TweetSentimentProcessor.TweetQueueName).getQueueUrl();
        System.out.println(queueUrl);
	}
	
	public void beginReceivingMessages() {
		System.out.println("Beginning execution");
		int id = 0;
		while (id++ <= 100) {
			executor.execute(new WorkerThread(id, sqs, queueUrl));
			
			
//	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(10);
//	        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("TweetId")).getMessages();
//	        for (Message message : messages) {
//	        	String tweetId = message.getMessageAttributes().get("TweetId").getStringValue();
//	        	executor.execute(new WorkerThread(tweetId, message.getBody()));
//	        }
//	        try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}
		executor.shutdown();
	}
	
	public static void main(String[] args) {
		TweetSentimentProcessor processor = new TweetSentimentProcessor();
		processor.beginReceivingMessages();
	}
}
