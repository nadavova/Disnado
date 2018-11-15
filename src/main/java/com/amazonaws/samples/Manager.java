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
	/*
	private static String mySendQueueUrl, myReceiveQueueUrl;
	private static String myReceiveQueueUrlName = "local_receive_manager_queue";
	private static String mySendQueueUrlName = "local_send_manager_queue";
	 */
	private static String sqsManagerWorkerNewTask = "sqsManagerWorkerNewTask";
	private static String sqsWorkerManagerDoneTask = "sqsWorkerManagerDoneTask";
	private static String sqsLocalManagerFileUpload = "sqsLocalManagerFileUpload";
	private static String sqsManagerLocalFileDone = "sqsManagerLocalFileDone";
	private static String myLocalSendQueueUrl, myReceiveQueueUrl;
	private static String myJobWorkerQueueUrl;
	private static String myDoneWorkerQueueUrl;
	private static List<Instance> workersList = new ArrayList<Instance>();
	private static int NumberOfWorkers = 0;
	public static void main(String[] args) throws IOException {
		BuildTools();
		
		Thread LocalManagerMessageReceiveThread = new Thread(() -> messageListener());
		LocalManagerMessageReceiveThread.start();
		//startWorkers(object);
		createWorkesrInstance();

	}

	private static void messageListener() throws IOException{
		S3Object object = getFile(s3);
		int numOfUrls = getNumOfUrls(object);
		startWorkers(numOfUrls);
		
		
		
	}

	private static void startWorkers(int numOfUrls) {
		int workersNeeded = numOfUrls / NumberOfWorkers;
		createWorkesrInstance(bucketname,workersNeeded);
		
	}

	private static int getNumOfUrls(S3Object object) throws IOException {
		System.out.println("\n object  : " +object);
		int counter = 0;
		//InputStream objectData = object.getObjectContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
		String line;
		while((line = reader.readLine()) != null) {
			counter++;

			//System.out.println(line);
		}
		return counter;
	}

	private static S3Object getFile(AmazonS3 s3) {
		System.out.println("getfile \n");
		//gets the file from local queue
		myReceiveQueueUrl = getQueue(sqsLocalManagerFileUpload);
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
	private static void createWorkesrInstance(String bucketname, int numOfWorkersToInitialize) {
		for(int i = 0 ; i<numOfWorkersToInitialize; i++) {
		RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", 1, 1);
		request.setInstanceType(InstanceType.T2Medium.toString());

		ArrayList<String> commands = new ArrayList<String>();
		commands.add("#!/bin/bash");
		commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
		commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
		commands.add("aws s3 cp s3://" + bucketname + "/worker.jar home/ec2-user/worker.jar");
		commands.add("yes | sudo yum install java-1.8.0");
		commands.add("yes | sudo yum remove java-1.7.0-openjdk");
		commands.add("sudo java -jar home/ec2-user/worker.jar"); 

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
		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
		for(Instance instance : instances) {
			CreateTagsRequest requestTag = new CreateTagsRequest();
			requestTag = requestTag.withResources(instance.getInstanceId())
					.withTags(new Tag("Worker", ""));
			ec2.createTags(requestTag);
			workersList.add(instance);
		}
		System.out.println("Launch instance: " + instances);
		NumberOfWorkers++;
	
		}
	}
}