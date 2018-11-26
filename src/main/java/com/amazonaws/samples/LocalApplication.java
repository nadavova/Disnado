package com.amazonaws.samples;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.FileWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.List;


import org.apache.commons.codec.binary.Base64;

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
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import static java.lang.Thread.sleep;

public class LocalApplication {
	protected static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	public static IamInstanceProfileSpecification IAMinstance;
	public static Message messageFromDoneQ;
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	private static String mySendQueueUrl, myReceiveQueueUrl;
	private static String sqsLocalManagerFileUpload = "sqsLocalManagerFileUpload";
	private static String sqsManagerLocalFileDone = "sqsManagerLocalFileDone";

	public static void main(String[] args) throws IOException, InterruptedException {
		long startTime = System.currentTimeMillis();
		boolean terminate = false;

		System.out.println("=====================bucketName: " + bucketName + "=====================");
		if(args[args.length - 1].equals("terminate")) {
			terminate = true;
		}
		buildTools();
		//createS3();
		//uploadFiles(args);
		//Instance managerInstance = createManagerInstance(Integer.parseInt(args[args.length - 1]));
		System.out.println("=====================Sending a message to Local-Manager Queue=====================");
		/* The application will send a message to a specified
		 *  SQS queue, stating the location of the images list on S3
		 */
		sqs.sendMessage(new SendMessageRequest(sqsLocalManagerFileUpload, "new task@@@" + bucketName + "@@@" + args[0] + "@@@" + args[args.length - 1]));
		try {
			while(!getResponseFromQ()) 
			{
				System.out.println("=====================wait, trying to get The Manager's Summary file=====================");
				sleep(10000);
			}
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		/*
		 * ****checks all messages from SQS****
		 * The application will check a specified SQS queue
		 *  for a message indicating the process is done 
		 *  and the response is available on S3.
		 */
		System.out.println("=====================RECEIVE MESSAGE TO DOWNLOAD=====================");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myReceiveQueueUrl);
		System.out.println("=====================MESSAGE RECEIVED=====================");
		if(messageFromDoneQ == null)
			System.out.println("=====================MessageFromDoneQ is null=====================");
		System.out.println("=====================MESSAGE ABOUT NEW UPLOAD(SUMMARY FILE) RECEIVED=====================");
		/*
		 * The application will download the response from S3.
		 */
		System.out.println("=====================DOWNLOADING RESPONSE HTML file FROM S3=====================");
		S3Object object;
		try {
			object = s3.getObject(new GetObjectRequest(bucketName, messageFromDoneQ.getBody().split("@@@")[2]));
			System.out.println("=====================SUMMARY OBJECT HAS BEEN DOWNLOADED=====================");
			String messageRecieptHandle = messageFromDoneQ.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(myReceiveQueueUrl, messageRecieptHandle));
			System.out.println("=====================MESSAGE ABOUT DONE SUMMARY FILE DELETED=====================");					
			System.out.println("=====================EXPORT SUMMARY FILE=====================");

			System.out.println("file name : " + messageFromDoneQ.getBody().split("@@@")[2]);
			try{
				// Create file 
				BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
				FileWriter fstream=new FileWriter(messageFromDoneQ.getBody().split("@@@")[2]);

				BufferedWriter out = new BufferedWriter(fstream);
				String line;
				while ((line = reader.readLine()) != null) {
					out.write(line);
				}
				//Close the output stream
				out.close();
			}catch (Exception e){//Catch exception if any
				System.err.println("Error: " + e.getMessage());
			}
		}
		catch (Exception e) {
			System.out.println("exception in getObject method");

		}
		System.out.println("=====================DOWNLOADING SUMMARY FILE TO LOCAL COMPUTER DONE=====================");
		System.out.println("=====================DELETE MESSAGE FROM LOCAL QUEUE=====================");
		s3.deleteObject(bucketName, messageFromDoneQ.getBody());

		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		System.out.println("=====================COMPLETED=====================");
	}

	private static void buildTools() {
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		//queue
		sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		mySendQueueUrl = getQueue(sqsLocalManagerFileUpload);
		myReceiveQueueUrl = getQueue(sqsManagerLocalFileDone);
		IAMinstance = new IamInstanceProfileSpecification();
		IAMinstance.setArn("arn:aws:iam::692054548727:instance-profile/EgorNadavRole");
	}

	private static boolean getResponseFromQ() throws InterruptedException {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myReceiveQueueUrl);
		//waitSomeTime();
		for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
			System.out.println("=====================RESPONSE RECEIVED=====================");
			messageFromDoneQ = message;
			return true;
		}
		System.out.println("waiting for response");
		return false;
	}

	private static Instance createManagerInstance(int workerCounter) {
		System.out.println("=====================Create Manager instance=====================");
		//get list of instances from aws
		DescribeInstancesRequest request1 = new DescribeInstancesRequest();
		DescribeInstancesResult response = ec2.describeInstances(request1);
		for(Reservation res : response.getReservations()) {
			List<Instance> instances = res.getInstances();
			for(Instance in: instances) {
				for (Tag tag: in.getTags()) { 
					if(tag.getKey().equals("Manager")) {
						if(!(in.getState().getName().equals("running"))) {
							// starts manager instance
							if(in.getState().getName().equals("terminated") || in.getState().getName().equals("shutting-down"))
								continue;
							List<String> temp = new ArrayList<String>();
							temp.add(in.getInstanceId());
							StartInstancesRequest req = new StartInstancesRequest(temp);
							ec2.startInstances(req);
						}
						return in;
					}
				}
			}
		}
		Instance instance = null;        
		try {
			//creates manager instance  
			RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", 1, 1);
			request.withKeyName("test");
			request.setIamInstanceProfile(IAMinstance);
			//request.withSecurityGroups("Nadav");
			request.setInstanceType(InstanceType.T2Micro.toString());
			ArrayList<String> commands = new ArrayList<String>();
			commands.add("#!/bin/bash\n"); //start the bash
			commands.add("sudo su\n");
			commands.add("yum -y install java-1.8.0 \n");
			commands.add("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
			commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
			commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
			commands.add("wget https://"+ bucketName + ".s3.amazonaws.com/" + "Manager.jar" +" -O ./" + "Manager.jar" );
			commands.add("java -jar Manager.jar");

			StringBuilder builder = new StringBuilder();

			Iterator<String> commandsIterator = commands.iterator();

			while (commandsIterator.hasNext()) {
				builder.append(commandsIterator.next());
				if (!commandsIterator.hasNext()) {
					break;
				}
				builder.append("\n");
			}
			String userData = new String(Base64.encodeBase64(builder.toString().getBytes()));
			request.setUserData(userData);
			instance = ec2.runInstances(request).getReservation().getInstances().get(0);
			CreateTagsRequest tagReq = new CreateTagsRequest();
			tagReq = tagReq.withResources(instance.getInstanceId()).withTags(new Tag("Manager", ""));
			ec2.createTags(tagReq);
			System.out.println("=====================Launch instance: " + instance + "=====================");
		} catch (AmazonServiceException ase) {
			System.out.println("Exception: Cannot create instance : "+ ase);
		}
		return instance;
	}

	private static void createS3() {
		System.out.println("=====================Create S3 storage=====================");
		for (Bucket bucket : s3.listBuckets()) {
			if(bucket.getName().equals(bucketName)) {
				System.out.println("=====================S3 Bucket exists=====================");
				return;
			}
		}
		try {
			System.out.println("=====================Creating bucket " + bucketName + "=====================");
			s3.createBucket(bucketName);
			System.out.println("=====================S3 bucket has been created=====================");

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
		}
	}
	private static String getQueue(String queueName) {
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


	//uploads 3 files, args[0] = input file , args[1] = Manager.jar file, args[2] = Worker.jar file
	private static void uploadFiles( String[] args) {     
		for( int i = 0; i < 3 ; i++) {
			System.out.println("Uploading jar files\n");
			String key = null;
			File file = null;

			file = new File(args[i]);
			key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
			System.out.println("key: " + key + "\n");
			System.out.println("file: " + file + "\n");
			PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
			req.setCannedAcl(CannedAccessControlList.PublicRead);

			//req.setCannedAcl(CannedAccessControlList.PublicRead);
			//The application will send a message to a specified SQS queue, stating the location of the images list on S3
			s3.putObject(req);
			System.out.println("after object request");
		}
	}
}
