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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
	public static IamInstanceProfileSpecification instanceP;
	public static boolean proceedThread2 = false;
	private static ArrayList<ArrayList<String>> ArrayDoneTasks = new ArrayList<ArrayList<String>>();
	static final Object lock = new Object();
	static final Object HTMLlock = new Object();




	private static class clientThread implements Runnable {
		private static int numOfUrlsPerWorker;
		private static int numberOfURLS = 0;
		private static int numOfDoneUrls = 0;
		private static String inputFileName;
		private static int NumberOfactiveWorkers = 0;
		private static ArrayList<String> processedUrlList = new ArrayList<String>();
		private static String fileNameRecievedFromWorker;

		@Override
		public void run() {
			try {
				synchronized (lock) {
					processedUrlList.add(inputFileName);
					ArrayDoneTasks.add(processedUrlList);
				}
				localMessageListener();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			workerMessageListener();
		}

		private static void localMessageListener() throws IOException{
			S3Object object = getFile();
			numberOfURLS = sendUrlsToMessageQueue(object);
			System.out.println("numberOfURLS : " + numberOfURLS);
			startWorkers();
		}

		private static void startWorkers() {
			System.out.println("startWorkers method");
			int workersNeeded = numberOfURLS / numOfUrlsPerWorker;// we get NumberOfMessagesPerWorker(n) from the message queue
			if(NumberOfactiveWorkers < workersNeeded) {
				for(int i = 0; i < workersNeeded - NumberOfactiveWorkers; i++)
					createWorkesrInstance(bucketName);//creates the missing workers
				NumberOfactiveWorkers = workersNeeded;
			}
		}
		private static List<Instance> createWorkesrInstance(String bucketname) {
			RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", 1, 1);
			request.setInstanceType(InstanceType.T2Micro.toString());
			ArrayList<String> commands = new ArrayList<String>();
			commands.add("#!/bin/bash\n"); //start the bash
			commands.add("sudo su\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
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
			request.setIamInstanceProfile(instanceP);
			String userData = new String(Base64.encode(builder.toString().getBytes()));
			request.setUserData(userData);
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			for(Instance instance : instances) {
				/*CreateTagsRequest requestTag = new CreateTagsRequest();
			requestTag = requestTag.withResources(instance.getInstanceId())
					.withTags(new Tag("Worker", ""));
			ec2.createTags(requestTag);
				 */
				//workersList.add(instance);

			}
			workersList.addAll(instances);
			System.out.println("Launch instance: " + instances);

			return instances;
		}

		private static void workerMessageListener() {
			System.out.println("num of urls to process is: " + numberOfURLS + "start while loop");
			getMessagesFromDoneTaskQ();

			File file =createHTML();

			uploadSummaryFileToS3(file);
			sendMessageToFileDoneQ();
			System.out.println("@@@@@MANAGER HAS DONE ALL TASKS@@@@@");

		}

		private static void getMessagesFromDoneTaskQ() {
			//processedUrl = new String[numberOfURLS];
			//UrlList = new String[numberOfURLS];
			System.out.println("numberOfURLS:  " + numberOfURLS);
			System.out.println("Waiting for response");
			while(numOfDoneUrls < numberOfURLS) {
				numOfDoneUrls = waitForResponses();
				
				System.out.println(numOfDoneUrls);
				//waitSomeTime();
				//System.out.println(sqs.receiveMessage(receiveMessageRequest).getMessages().toString());
				/*for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
				System.out.println("message (WorkerMessageListener) : " + message);
				//gets the done url
				processedUrl[numOfDoneUrls]= message.getBody().split("@@@")[1];
				UrlList[numOfDoneUrls]= message.getBody().split("@@@")[2];

				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(myDoneWorkerQueueUrl, messageRecieptHandle));
				numOfDoneUrls++;
			}*/
			}
			/*for(int i = 0; i<processedUrl.length;i++) {
				System.out.println("processedUrl["+i+"]: " +processedUrl[i]);
			}
			for(int i = 0; i<UrlList.length;i++) {
				System.out.println("UrlList["+i+"]: " +UrlList[i]);
			}*/
			//closeInstances();


		}

		private static int waitForResponses() {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myDoneWorkerQueueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			String[] response;
			if (messages.size() > 0) {
				for (int i = 0; i < messages.size(); i++) {
						System.out.println("GOT RESPONSE!");
						response=messages.get(i).getBody().split("@@@");
						fileNameRecievedFromWorker = response[3];
						for(ArrayList<String>  temp : ArrayDoneTasks) {
							if(temp.contains(fileNameRecievedFromWorker)) {
								temp.add(response[1] + "@@@" + response[2]);
							}
						}

						//processedUrl[numOfDoneUrls]= messages.get(i).getBody().split("@@@")[1];
						//UrlList[numOfDoneUrls]= messages.get(i).getBody().split("@@@")[2];
						String reciptHandleOfMsg = messages.get(i).getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(myDoneWorkerQueueUrl, reciptHandleOfMsg));
						numOfDoneUrls++;
					}
				}
			
			else
				System.out.println("NO RESPONSE");
			return numOfDoneUrls;
		}
		
		private static void uploadSummaryFileToS3(File HTMLsummaryFile) {
			System.out.println("UPLOAD SUMMARY FILE TO S3");
			PutObjectRequest req = new PutObjectRequest(bucketName, "output"+fileNameRecievedFromWorker + ".html", HTMLsummaryFile);
			s3.putObject(req);
			System.out.println("UPLOAD SUMMARY FILE TO S3 COMPLETED");

		}

		private static File createHTML() {
			synchronized (HTMLlock) {
				System.out.println("@@@@@@@@@@CREATE HTML@@@@@@@@@@@@@@@@@@@@");

				try {
					File file = new File ("output"+fileNameRecievedFromWorker + ".html");
					PrintWriter writer = new PrintWriter(file);
					writer.write("<!DOCTYPE html>\n" + "<html>\n" + "<body>\n");
					//System.out.println("URLLIST.LENGTH = " +UrlList.length );
					//Dummy implementation of map... too lazy
					for(ArrayList<String>  temp : ArrayDoneTasks) {
						if(temp.contains(fileNameRecievedFromWorker)) {//bug in here
							temp.remove(fileNameRecievedFromWorker);
							for(String temp2 : temp) {
								System.out.println(" temp2.split(\"@@@\")[1]: " +  temp2.split("@@@")[1] + "temp2.split)[0]: " + temp2.split("@@@")[0]);

								writer.write("<p><img src=" + temp2.split("@@@")[1] + "><br>");
								//System.out.println(" temp2.split(\"@@@\")[2]: " +  "temp2.splsit(\"@@@\")[1]: " + temp2.split("@@@")[1]);
								writer.write("\n<p>");
								writer.write(temp2.split("@@@")[0]);
							}
						}			
					}

					/*for(int i = 0; i< UrlList.length;i++) {
					System.out.println("INSIDE CREATE HTML LOOP, i = " + i);

					writer.write("<p><img src=" + UrlList[i] + "><br>");
					writer.write("\n<p>");
					writer.write(processedUrl[i]);

				}*/
					writer.write("</body>\n" + "</html>");
					writer.close();
					System.out.println("Creation of HTML file was completed");
					System.out.println("file.toString : " + file.toString());
					return file;


				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Error on creating HTML file");
			}
			return null;

		}

		private static void sendMessageToFileDoneQ() {
			System.out.println("SENDING MESSAGE TO DONE Q");

			myReceiveQueueUrl = createAndGetQueue(sqsManagerLocalFileDone);
			String doneTask = "done task@@@" + bucketName + "@@@" + fileNameRecievedFromWorker;
			System.out.println("done tasK");
			sqs.sendMessage(new SendMessageRequest(sqsManagerLocalFileDone, doneTask));
			System.out.println("THE DONE MESSAGE HAS BEEN SENT PROPERLY");

		}

		//sends new tasks(urls) to message queue and returns number of urls to be done.
		private static int sendUrlsToMessageQueue(S3Object object) throws IOException {
			System.out.println("\n object  : " +object);
			int counter = 0;
			//InputStream objectData = object.getObjectContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			String line;

			while((line = reader.readLine()) != null) {
				System.out.println("----------------------------------------------counter: " + counter + "line: " + line+"----------------------------------------------");
				// send new worker job to sqsManagerWorkerNewTask queue
				sqs.sendMessage(new SendMessageRequest(sqsManagerWorkerNewTask, "new image task@@@" + bucketName + "@@@" + line + "@@@" + inputFileName));
				counter++;


			}
			return counter;
		}

		private static S3Object getFile() {
			System.out.println("getfile \n");

			S3Object object;
			try {
				System.out.println(" before getobject ");
				//System.out.println(message.getBody().substring(message.getBody().lastIndexOf('/') + 1));
				object = s3.getObject(new GetObjectRequest(bucketName, inputFileName));

				return object;

			}
			catch (Exception e) {

			}
			return null;

		}


		private static void waitSomeTime() {
			try {
				System.out.println("wait loop - trying to get all The Workers OCR Text..");
				sleep(8000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}



		/*private static void closeInstances() {
        List<String> toCloseList = new ArrayList<>();
        if (workersList != null) {
            for (Instance i : workersList)
                toCloseList.add(i.getInstanceId());
            TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(toCloseList);
            ec2.terminateInstances(terminateRequest);
        }
    }*/
	}
	public static void main(String[] args) throws IOException, Exception {
		BuildTools();
		while(true) {
			getNewMessageAndRunThread();
			//System.out.println("wait loop - trying to get all The Workers OCR Text..");
			//need to terminate manager here
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
		//creates Manager->Worker queue in order to send new tasks to worker
		myJobWorkerQueueUrl = createAndGetQueue(sqsManagerWorkerNewTask);
		myReceiveQueueUrl = createAndGetQueue(sqsLocalManagerFileUpload);
		myDoneWorkerQueueUrl = createAndGetQueue(sqsWorkerManagerDoneTask);



		instanceP = new IamInstanceProfileSpecification();
		instanceP.setArn("arn:aws:iam::692054548727:instance-profile/EgorNadavRole");

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
					client.numOfUrlsPerWorker = Integer.parseInt(parseMessage[3]);
					//String fullPath = "C:\\Hello\\AnotherFolder\\The File Name.PDF";
					//int index = fullPath.lastIndexOf("\\");
					//String fileName = fullPath.substring(index + 1);
					System.out.println("parseMessage[2]: " + parseMessage[2]);
					client.inputFileName = parseMessage[2].substring(parseMessage[2].lastIndexOf("\\")+1);
					System.out.println(" inputFileName" + client.inputFileName);

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
