package com.amazonaws.samples;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.BufferedReader;
import java.util.List;

public class Manager {
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static String mySendQueueUrl, myReceiveQueueUrl;
	private static String myReceiveQueueUrlName = "local_receive_manager_queue";
	private static String mySendQueueUrlName = "local_send_manager_queue";

	public static void main(String[] args) throws IOException {
		BuildTools();
		S3Object object = getFile(s3);
		//startWorkers(object);
		createWorkesrInstance(2);
		
	}

	private static void startWorkers(S3Object object) throws IOException {
		System.out.println("\n object  : " +object);
		//InputStream objectData = object.getObjectContent();
	    BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
	    String line;
	    while((line = reader.readLine()) != null) {
	    	

	      System.out.println(line);
	    }
	    
		
	}

	private static S3Object getFile(AmazonS3 s3) {
		System.out.println("getfile \n");
		myReceiveQueueUrl = getQueue(mySendQueueUrlName);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myReceiveQueueUrl);
		//System.out.println(sqs.receiveMessage(receiveMessageRequest).getMessages().toString());
		for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
			System.out.println("message : " + message);
			if(message == null)
				continue;
			else {
				
				S3Object object;
				try {
					System.out.println(" before getobject ");
					System.out.println(message.getBody().substring(message.getBody().lastIndexOf('/') + 1));
					object = s3.getObject(new GetObjectRequest(bucketName, "input.txt"));
					return object;

				}
				catch (Exception e) {
					continue;
				}
			}
		}
		return null;
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
	private static String getQueue(String queueName) {
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			if(queueName.equals(queueUrl.substring(queueUrl.lastIndexOf('/') + 1)))
				return queueUrl;
		}
		return null;
	}
	private static List<Instance> createWorkesrInstance(int workerCounter) {
		Instance instance = null;   
		RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", 1, 1);
		request.setInstanceType(InstanceType.T1Micro.toString());
		request.setMinCount(workerCounter);
		request.setMaxCount(workerCounter);
		/*ArrayList<String> commands = new ArrayList<String>();

		StringBuilder builder = new StringBuilder();

		Iterator<String> commandsIterator = commands.iterator();

		while (commandsIterator.hasNext()) {
			builder.append(commandsIterator.next());
			if (!commandsIterator.hasNext()) {
				break;
			}
			builder.append("\n");
		}

		String userData = new String(Base64.encode(builder.toString().getBytes()));
		request.setUserData(userData);
		*/
		instance = ec2.runInstances(request).getReservation().getInstances().get(0);
		CreateTagsRequest request7 = new CreateTagsRequest();
		request7 = request7.withResources(instance.getInstanceId())
				.withTags(new Tag("Worker", ""));
		ec2.createTags(request7);
		System.out.println("Launch instance: " + instance);
		// start instance
		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
		return instances;
		
	}
}