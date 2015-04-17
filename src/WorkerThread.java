
public class WorkerThread implements Runnable {
	private long tweetId;
	private String message;

    public WorkerThread(String idString, String msg) {
    	tweetId = Long.parseLong(idString);
    	message = msg;
    }

    public void run() {
    	System.out.println(tweetId + " : " + message);
    }
}
