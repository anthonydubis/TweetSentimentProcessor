import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class WorkerThread implements Runnable {
	private static final String apiKey = "4c4269e62c0a19c4fdc84b78093428881101a14b";
	
	private AmazonSQS sqs;
	private String queueUrl;
	private Message message;
	private Double sentiment;
	private DBHelper dbHelper;
	
	public WorkerThread(AmazonSQS sqs, String queueUrl, DBHelper dbHelper) {
		this.sqs = sqs;
		this.queueUrl = queueUrl;
		this.dbHelper = dbHelper;
	}
	
	private Message getAMessage() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).
				withMaxNumberOfMessages(1).
				withWaitTimeSeconds(2);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("TweetId")).getMessages();
        if (messages.size() > 0) {
        	return messages.get(0);
        } else {
        	return null;
        }
	}
	
	private static double parseOutSentiment(String xml) {
		double sentiment = 0.0;
		int startIdx = xml.indexOf("<score>");
		int endIdx = xml.indexOf("</score>");
		if (startIdx >= 0 && endIdx >= 0) {
			sentiment = Double.parseDouble(xml.substring(startIdx + "<score>".length(), endIdx));
		}
		return sentiment;
	}
	
	/*
	 * Get the sentiment of the tweet's text
	 */
	private Double getSentiment(String text) 
			throws URISyntaxException, ClientProtocolException, IOException {
		Double result = 0.0;
		// Build the URI
		URI uri = new URIBuilder()
			.setScheme("http")
			.setHost("access.alchemyapi.com/")
			.setPath("/calls/text/TextGetTextSentiment")
			.setParameter("apikey", WorkerThread.apiKey)
			.setParameter("text", text)
			.build();

		// Create the HttpGet
		HttpGet httpget = new HttpGet(uri);
		
		// Execute the client
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response = httpclient.execute(httpget);
		try {
		    HttpEntity entity = response.getEntity();
		    if (entity != null) {
		    	String xml = EntityUtils.toString(entity);
		    	result = parseOutSentiment(xml);
		    }
		} finally {
		    response.close();
		}
		return result;
	}
	
	public void run() {
		// Try to get a message - return if you can't
		message = getAMessage();
		if (message == null) return;
		
		// Get the message sentiment
		try {
			sentiment = getSentiment(message.getBody());
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Insert the sentiment into the database
		long tweetId = Long.parseLong(message.getMessageAttributes().get("TweetId").getStringValue());
		dbHelper.updateSentiment(tweetId, sentiment);		
		System.out.println("WORKER: TweetID: " + tweetId + ", sentiment: " + sentiment);
	
		// Send the SNS message
		
        // Delete the message so it isn't analyzed again
       	String messageRecieptHandle = message.getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
	}
}
