package com.amazonaws.samples;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import static java.lang.Thread.sleep;


public class Manager {
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;


	private static String sqsManagerWorkerNewTask = "sqsManagerWorkerNewTask";
	private static String sqsWorkerManagerDoneTask = "sqsWorkerManagerDoneTask";
	private static String sqsLocalManagerFileUpload = "sqsLocalManagerFileUpload";
	private static String sqsManagerLocalFileDone = "sqsManagerLocalFileDone";
	private static String  myReceiveQueueUrl, myJobWorkerQueueUrl, myDoneWorkerQueueUrl, mySendQueueUrl;
	//private static List<Instance> workersList = new ArrayList<Instance>();
	public static IamInstanceProfileSpecification instanceP;
	public static boolean proceedThread2 = false;
	private static ArrayList<ArrayList<String>> ArrayDoneTasks = new ArrayList<ArrayList<String>>();
	static final Object lock = new Object();
	static final Object HTMLlock = new Object();
	static final Object workerLock = new Object();


	private static class clientThread implements Runnable {
		private static int numOfUrlsPerWorker;
		private static int NumberOfactiveWorkers ;
		private static int numberOfURLS ;
		private static int numOfDoneUrls;
		private static String fileNameRecievedFromWorker;
		private static String inputFileName;
		private static ArrayList<String> processedUrlList ;
		private static List<Instance> workersList ;

		@Override
		public void run() {
			try {
				synchronized (lock) {
					processedUrlList.add(inputFileName);
					ArrayDoneTasks.add(processedUrlList);
				}
				localMessageListener();
			} catch (IOException e) {
				System.out.println("=====================Could not catch lock=====================");
				e.printStackTrace();
			}
			workerMessageListener();
			Thread.currentThread().interrupt();
		}

		private static void localMessageListener() throws IOException{
			S3Object object = getFile();
			numberOfURLS = sendUrlsToMessageQueue(object);
			System.out.println("numberOfURLS : " + numberOfURLS);
			startWorkers();
		}

		private static void startWorkers() {
			synchronized(workerLock) {
				System.out.println("=====================startWorkers method=====================");
				System.out.println("NumberOfactiveWorkers: " + NumberOfactiveWorkers);
				System.out.println("numberOfURLS: " + numberOfURLS);
				System.out.println("numOfUrlsPerWorker: " + numOfUrlsPerWorker);
				if(NumberOfactiveWorkers < (numberOfURLS / numOfUrlsPerWorker)) {
					for(int i = 0; i < (numberOfURLS / numOfUrlsPerWorker) - NumberOfactiveWorkers; i++)
						//creates the missing workers
						createWorkesrInstance();
					NumberOfactiveWorkers = numberOfURLS / numOfUrlsPerWorker;
				}}
		}
		private static List<Instance> createWorkesrInstance() {
			try {
				RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", 1, 1);
				request.setInstanceType(InstanceType.T2Micro.toString());
				request.setIamInstanceProfile(instanceP);
				ArrayList<String> commands = new ArrayList<String>();
				//Start worker script
				commands.add("#!/bin/bash\n"); 
				commands.add("sudo su\n");
				commands.add("yum -y install java-1.8.0 \n");
				commands.add("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
				commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
				commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
				commands.add("wget https://"+ bucketName + ".s3.amazonaws.com/" + "Worker.jar" +" -O ./" + "Worker.jar" );
				commands.add("java -jar Worker.jar");

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
				workersList.addAll(instances);
				System.out.println("=====================Launch instance: " + instances + "=====================");
				return instances;
			}
			catch(AmazonServiceException ase) {
				System.out.println("Exception: (Error while trying to run new instance) " + ase.getMessage());
			}
			return null;
		}

		private static void workerMessageListener() {
			System.out.println("Number of URLs to process is: " + numberOfURLS);
			getMessagesFromDoneTaskQ();
			File file =createHTML();
			String name = uploadSummaryFileToS3(file);
			sendMessageToFileDoneQ(name);
			System.out.println("=====================MANAGER HAS DONE ALL TASKS=====================");
			closeInstances();
		}

		private static void getMessagesFromDoneTaskQ() {
			System.out.println("=====================numberOfURLS:  " + numberOfURLS + "=====================");
			System.out.println("=====================Waiting for response=====================");
			while(numOfDoneUrls < numberOfURLS) {
				numOfDoneUrls = waitForResponses();
				System.out.println(numOfDoneUrls);
			}
		}

		private static int waitForResponses() {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myDoneWorkerQueueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			String[] response;
			if (messages.size() > 0) {
				for (int i = 0; i < messages.size(); i++) {
					System.out.println("=====================GOT RESPONSE!=====================");
					response=messages.get(i).getBody().split("@@@");
					System.out.println("numOfUrlsPerWorker:" + numOfUrlsPerWorker);
					fileNameRecievedFromWorker = response[3];
					for(ArrayList<String>  temp : ArrayDoneTasks) {
						if(temp.contains(fileNameRecievedFromWorker)) {
							temp.add(response[1] + "@@@" + response[2]);
						}
					}
					String reciptHandleOfMsg = messages.get(i).getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(myDoneWorkerQueueUrl, reciptHandleOfMsg));
					numOfDoneUrls++;
				}
			}
			//else
			//System.out.println("=====================NO RESPONSE=====================");
			return numOfDoneUrls;
		}

		private static String uploadSummaryFileToS3(File HTMLsummaryFile) {
			System.out.println("=====================UPLOAD SUMMARY FILE TO S3=====================");
			String name = "output"+fileNameRecievedFromWorker + ".html";
			PutObjectRequest req = new PutObjectRequest(bucketName, name, HTMLsummaryFile);
			s3.putObject(req);
			System.out.println("=====================UPLOAD SUMMARY FILE TO S3 COMPLETED=====================");
			return name;
		}

		private static File createHTML() {
			synchronized (HTMLlock) {
				System.out.println("=====================CREATE HTML=====================");
				try {
					File file = new File ("output"+fileNameRecievedFromWorker + ".html");
					PrintWriter writer = new PrintWriter(file);
					writer.write("<!DOCTYPE html>\n" + "<html>\n" + "<body>\n");
					//Dummy implementation of map... too lazy
					for(ArrayList<String>  temp : ArrayDoneTasks) {
						if(temp.contains(fileNameRecievedFromWorker)) {
							temp.remove(fileNameRecievedFromWorker);
							for(String temp2 : temp) {
								writer.write("<p><img src=" + temp2.split("@@@")[1] + "><br>");
								writer.write("\n<p>");
								writer.write(temp2.split("@@@")[0]);
							}
						}			
					}
					writer.write("</body>\n" + "</html>");
					writer.close();
					System.out.println("=====================Creation of HTML file  completed=====================");
					return file;
				} catch (FileNotFoundException e) {
					System.out.println("Exception : could not find file");
					e.printStackTrace();
				}
				System.out.println("=====================Error on creating of HTML file=====================");
			}
			return null;
		}

		private static void sendMessageToFileDoneQ(String summaryFileName) {
			System.out.println("=====================SENDING MESSAGE TO DONE Queue=====================");
			myReceiveQueueUrl = createAndGetQueue(sqsManagerLocalFileDone);
			String doneTask = "done task@@@" + bucketName + "@@@" + summaryFileName;
			sqs.sendMessage(new SendMessageRequest(sqsManagerLocalFileDone, doneTask));
			System.out.println("=====================THE DONE MESSAGE HAS BEEN SENT PROPERLY=====================");
		}

		//sends new tasks(urls) to message queue and returns number of urls to be done.
		private static int sendUrlsToMessageQueue(S3Object object) throws IOException {
			int counter = 0;
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			String line;
			while((line = reader.readLine()) != null) {
				// send new worker job to sqsManagerWorkerNewTask queue
				sqs.sendMessage(new SendMessageRequest(sqsManagerWorkerNewTask, "new image task@@@" + bucketName + "@@@" + line + "@@@" + inputFileName));
				counter++;
			}
			return counter;
		}

		private static S3Object getFile() {
			System.out.println("=====================Getting file from S3=====================");
			S3Object object;
			try {
				object = s3.getObject(new GetObjectRequest(bucketName, inputFileName));
				return object;
			}
			catch (Exception e) {
				System.out.println("Exception : Error on getObject method, file doesnt exists");
			}
			return null;

		}

		private static void closeInstances() {
			System.out.println("==================Shutting down working instances====================");
			List<String> toCloseList = new ArrayList<>();
			if (workersList != null) {
				for (Instance i : workersList) {
					toCloseList.add(i.getInstanceId());
					System.out.println("==================Shutting " + i.getInstanceId() +"====================");
				}
				TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(toCloseList);
				ec2.terminateInstances(terminateRequest);
			}
		}
	}

	public static void main(String[] args) throws IOException, Exception {
		BuildTools();
		while(true) {
			getNewMessageAndRunThread();
			try {
				System.out.println("wait....");
				sleep(8000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//System.out.println("wait loop - trying to get all The Workers OCR Text..");
			//need to terminate manager here
		}
		//deleteQueue();
		//deleteS3AndContent();
	}
	private static void deleteS3AndContent() {
		ObjectListing objectListing = s3.listObjects(bucketName);
		while (true) {
			Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
			while (objIter.hasNext()) {
				s3.deleteObject(bucketName, objIter.next().getKey());
			}
			break;
		}
		s3.deleteBucket(bucketName);
	}
	private static void deleteQueue() {
		sqs.deleteQueue(new DeleteQueueRequest(myDoneWorkerQueueUrl));
		sqs.deleteQueue(new DeleteQueueRequest(myJobWorkerQueueUrl));
		sqs.deleteQueue(new DeleteQueueRequest(myReceiveQueueUrl));
		sqs.deleteQueue(new DeleteQueueRequest(mySendQueueUrl));
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
		//creates Manager->Worker queue in order to send new tasks to worker
		myJobWorkerQueueUrl = createAndGetQueue(sqsManagerWorkerNewTask);
		myReceiveQueueUrl = createAndGetQueue(sqsLocalManagerFileUpload);
		myDoneWorkerQueueUrl = createAndGetQueue(sqsWorkerManagerDoneTask);

		instanceP = new IamInstanceProfileSpecification();
		instanceP.setArn("arn:aws:iam::692054548727:instance-profile/NadavS3Role");

	}
	private static void getNewMessageAndRunThread() {
		String[] parseMessage = null;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myReceiveQueueUrl);
		for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
			System.out.println("message : " + message);
			if(message == null)
				continue;
			else {
				if (message.getBody().startsWith("new task@@@")) {
					parseMessage = message.getBody().split("@@@");
					clientThread client = new clientThread();
					client.NumberOfactiveWorkers=0;
					client.numOfUrlsPerWorker = Integer.parseInt(parseMessage[3]);
					client.inputFileName = parseMessage[2].substring(parseMessage[2].lastIndexOf("\\")+1);
					client.numberOfURLS = 0 ;
					client.numOfDoneUrls = 0;
					client.fileNameRecievedFromWorker = "";
					client.processedUrlList = new ArrayList<String>();
					client.workersList = new ArrayList<Instance>();
					System.out.println("=====================input File Name: " + client.inputFileName + "=====================");
					new Thread(client).start();
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(myReceiveQueueUrl, messageRecieptHandle));
				}
			}
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
	}}
