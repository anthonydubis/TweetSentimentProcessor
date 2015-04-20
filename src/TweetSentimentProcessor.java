
import java.util.concurrent.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;


public class TweetSentimentProcessor {
	private static final String TweetQueueName = "TweetQueue";
	private static final String arn = "arn:aws:sns:us-east-1:461013519714:TestTop5";
	
	private final int MaxQueueSize = 100;
	private final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(MaxQueueSize);
	private final int NumberDBHelpers = 10;
	private final int ThreadPoolSize = 10;
	private AmazonSQS sqs;
	private AmazonSNSClient snsClient;
	private String queueUrl;
	private ExecutorService executor = new ThreadPoolExecutor(ThreadPoolSize, ThreadPoolSize, 
			0L, TimeUnit.MILLISECONDS, queue);
	private DBHelper[] dbHelpers = new DBHelper[NumberDBHelpers];
	
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
        
        // Setup the SNS Client
        snsClient = new AmazonSNSClient(credentials);
	}
	
	public void sentimentReceived(long tweetId, double sentiment) {
		// System.out.println("OWNER: TweetID: " + tweetId + ", sentiment rating: " + sentiment);
	}
	
	public void beginReceivingMessages() {
		int id = 0;
		while (true) {
			if (queue.size() == MaxQueueSize)
				try {
					System.out.println("Sleeping - Send SNS ***********************************");
					// Send the SNS message
			        PublishRequest request = new PublishRequest(TweetSentimentProcessor.arn, "UpdateSentimentAttributes");
			        snsClient.publish(request);
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			else
				executor.execute(new WorkerThread(sqs, queueUrl, dbHelpers[id % NumberDBHelpers]));
		}
		// executor.shutdown();
	}
	
	public void setupDBHelpers() {
		for (int i = 0; i < NumberDBHelpers; i++) {
			dbHelpers[i] = new DBHelper();
		}
	}
	
	public static void main(String[] args) {
		TweetSentimentProcessor processor = new TweetSentimentProcessor();
		processor.setupDBHelpers();
		processor.beginReceivingMessages();
	}
}
