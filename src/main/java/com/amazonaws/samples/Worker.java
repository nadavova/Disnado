import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import com.asprise.ocr.Ocr;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Worker {
	private static String sqsManagerWorkerNewTask = "sqsManagerWorkerNewTask";
	private static String sqsWorkerManagerDoneTask = "sqsWorkerManagerDoneTask";
	private static String myJobWorkerQueueUrl;
	private static String myDoneWorkerQueueUrl;
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String URLToParse = null;
	private static String outputText = null;
	private static String fileName = null;

	/*Worker functions:
	 *Get an image message from an SQS queue.
	 *Download the image file indicated in the message.
	 *Apply OCR on the image.
	 *Notify the manager of the text associated with that image.
	 *remove the image message from the SQS queue.
	 **/
	
	public static void main(String[]args) {
		System.out.println("=====================STARTING WORKER=====================");
		BuildTools();
		while(getMessageFromManagerQ()) {
			doOcr();
			sendProcessedMessageToManagerQ();
		}
	}
	
	//Notify the manager of the text associated with that image.
	private static void sendProcessedMessageToManagerQ() {
		System.out.println("=====================SEND PROCESSED MESSAGE TO QUEUE=====================");
		// message : [0]done image task, [1] processed text [2]URL [3]filename
		sqs.sendMessage(new SendMessageRequest(sqsWorkerManagerDoneTask, "done image task@@@" +outputText +"@@@" + URLToParse + "@@@" + fileName ));
	}
	
	private static void BuildTools() {
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		
		myDoneWorkerQueueUrl = createAndGetQueue(sqsWorkerManagerDoneTask);
		myJobWorkerQueueUrl = createAndGetQueue(sqsManagerWorkerNewTask);
	}
	//Get an image message from an SQS queue
	private static boolean getMessageFromManagerQ() {
		String[] parseMessage = null;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myJobWorkerQueueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		int i = 0;
		for (Message message : messages) {
			parseMessage = message.getBody().split("@@@");
			URLToParse = parseMessage[2];
			fileName = parseMessage[3];
			System.out.println(i + ") URLtoParse : " + URLToParse);
			//remove the image message from the SQS queue.
			System.out.println("=====================DELETE MESSAGE FROM QUEUE : " + message + "=====================");
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myJobWorkerQueueUrl, messageRecieptHandle));
			return true;
		}
		System.out.println("=====================RETURNS FALSE=====================");
		return false;	
	}
	//Apply OCR on the image.
	public static void doOcr() {
        Ocr.setUp();
        Ocr ocr = new Ocr();
        ocr.startEngine("eng", Ocr.SPEED_FASTEST);
        InputStream in;
        try {
        	//Download the image file indicated in the message.
            URL url = new URL(URLToParse);
            in=url.openStream();
            Files.copy(in,Paths.get("tempImage.jpg"),StandardCopyOption.REPLACE_EXISTING);
            in.close();
            outputText= ocr.recognize(new File[] { new File ("tempImage.jpg")}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
            System.out.println("Text after OCR process: \n "+ outputText);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ocr.stopEngine();
		
	}
	
	private static String createAndGetQueue(String queueName) {
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			if(queueName.equals(queueUrl.substring(queueUrl.lastIndexOf('/') + 1)))
				return queueUrl;
		}
		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			return sqs.createQueue(createQueueRequest).getQueueUrl();
		}
		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
		} 
		catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
		}
		return null;
	}
}
