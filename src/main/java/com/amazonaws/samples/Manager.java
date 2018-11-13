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


public class Manager {
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	private static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;

	public static void main(String[] args) throws IOException {
		BuildTools();
		getFile(s3);

	}

	private static void getFile(AmazonS3 s3) {
		for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
			if(message == null)
				continue;
			else {
				S3Object object;
				try {
					object = s3.getObject(new GetObjectRequest(bucketName, message.getBody()));
				}
				catch (Exception e) {
					continue;
				}
			}
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
}