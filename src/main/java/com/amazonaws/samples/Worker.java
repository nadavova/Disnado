package com.amazonaws.samples;

import java.io.File;
import java.util.List;

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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class Worker {
	private static String sqsManagerWorkerNewTask = "sqsManagerWorkerNewTask";
	private static String sqsWorkerManagerDoneTask = "sqsWorkerManagerDoneTask";
	private static String myJobWorkerQueueUrl;
	private static String myDoneWorkerQueueUrl;
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	
	public static void main(String[]args) {
		BuildTools();
		while(true) {
			getMessageFromManagerQ();
		}
		
		
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

	}

	private static void getMessageFromManagerQ() {
		String[] parseMessage = null;
		myJobWorkerQueueUrl = createAndGetQueue(sqsManagerWorkerNewTask);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myJobWorkerQueueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		for (Message message : messages) {
			parseMessage = message.getBody().split("@@@");
			URLToParse = parseMessage[2];
			
			//reciptHandleOfMsg=message.getReceiptHandle();
	        //removeImageFromQueue();
			
		}
	    private static void handleTask() {
	        Ocr.setUp();
	        Ocr ocr = new Ocr();
	        ocr.startEngine("eng", Ocr.SPEED_FASTEST);
	        InputStream in;
	        try {
	            URL url = new URL(imageURL);
	            in=url.openStream();
	            Files.copy(in,Paths.get("pic.jpg"),StandardCopyOption.REPLACE_EXISTING);
	            in.close();
	            textOfImage= ocr.recognize(new File[] { new File ("pic.jpg")}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
	            System.out.println("Got this text: \n "+ textOfImage);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        ocr.stopEngine();
	    }
			
			
		

		
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
