package com.example.aws_sdk;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSS3CrudApp {
	public static void main(String[] args) throws InterruptedException, IOException {
		String clientRegion = "ap-south-1";
		String bucketName = "demo-sdk-bucket-0306";

		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();

		CreateBucket(amazonS3, bucketName);
		System.out.println("Waiting.......");
		TimeUnit.SECONDS.sleep(5);
		ListBucket(amazonS3);
		System.out.println("Waiting.......");
		TimeUnit.SECONDS.sleep(5);
		putObjectInBucket(amazonS3, bucketName);
		System.out.println("Waiting.......");
		TimeUnit.SECONDS.sleep(5);
		listObject(amazonS3, bucketName);
		System.out.println("Waiting.......");
		TimeUnit.SECONDS.sleep(5);
		downloadObject(amazonS3, bucketName);
		copyObject(amazonS3, bucketName);
		System.out.println("waiting........");
		TimeUnit.SECONDS.sleep(5);
		deleteObject(amazonS3, bucketName);
		System.out.println("waiting........");
		TimeUnit.SECONDS.sleep(5);
		deleteBucket(amazonS3, bucketName);
	}

	private static void deleteBucket(AmazonS3 amazonS3, String bucketName) {

		List<Bucket> buckets = ListBucket(amazonS3);
		if (!buckets.isEmpty()) {
			for (Bucket b : buckets) {
				List<S3ObjectSummary> objects = listObject(amazonS3, b.getName());
				if (objects.size() != 0) {
					List<S3ObjectSummary> objectList = listObject(amazonS3, b.getName());
					for (S3ObjectSummary summary : objectList) {
						amazonS3.deleteObject(b.getName(), summary.getKey());
					}

				}
				System.out.println("deleting bucket " + b.getName());
				amazonS3.deleteBucket(b.getName());
			}
		}
		System.out.println("deleted successfully ............");
		System.out.println("===================================================");
		System.out.println(ListBucket(amazonS3).size());
	}

	private static void deleteObject(AmazonS3 amazonS3, String bucketName) {

		System.out.println("deleting object from bucket " + bucketName);

		amazonS3.deleteObject(bucketName, "Document/hello.txt");

		System.out.println("Object deleted .................");
		System.out.println("=================================");

		listObject(amazonS3, bucketName);
	}

	private static void copyObject(AmazonS3 amazonS3, String bucketName) {
		System.out.println("copying object from bucket " + bucketName + " to " + bucketName + 0);
		amazonS3.copyObject(bucketName, "Document/hello.txt", bucketName + 0, "Document/hello1.txt");
		System.out.println("object copied......");
		System.out.println("============================================");
		listObject(amazonS3, bucketName);
		listObject(amazonS3, bucketName + 0);
	}

	private static void downloadObject(AmazonS3 amazonS3, String bucketName) throws IOException {

		System.out.println("downloading object from bucket " + bucketName);

		S3Object s3Object = amazonS3.getObject(bucketName, "Document/hello.txt");
		S3ObjectInputStream inputStream = s3Object.getObjectContent();
		FileUtils.copyInputStreamToFile(inputStream, new File("C:/Users/het.shah/Desktop/hello1.txt"));
		System.out.println("Object downloaded.........");
		System.out.println("================================================================");

	}

	private static List<S3ObjectSummary> listObject(AmazonS3 amazonS3, String bucketName) {

		System.out.println("listing object from " + bucketName);

		ObjectListing objectListing = amazonS3.listObjects(bucketName);
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			System.out.println(objectSummary.getKey());
		}
		System.out.println("==============================================================");

		return objectListing.getObjectSummaries();
	}

	private static void putObjectInBucket(AmazonS3 amazonS3, String bucketName) {

		System.out.println("putting an object in " + bucketName + " bucket");
		amazonS3.putObject(bucketName, "Document/hello.txt", new File("C:/Users/het.shah/Desktop/hello.txt"));
		System.out.println("File uploaded......");
		System.out.println("==============================================================");
	}

	private static List<Bucket> ListBucket(AmazonS3 amazonS3) {

		System.out.println("Listing buckets..........");

		List<Bucket> buckets = amazonS3.listBuckets();
		for (Bucket b : buckets) {
			System.out.println(b.getName());
		}
		System.out.println("==============================================================");

		return buckets;
	}

	private static void CreateBucket(AmazonS3 amazonS3, String bucketName) {

		System.out.println("Creating bucket....");
		amazonS3.createBucket(bucketName);
		System.out.println("Bucket with name " + bucketName + " created successfully");
		int i = 0;

		while (i < 3) {
			if (!amazonS3.doesBucketExistV2(bucketName + i)) {
				amazonS3.createBucket(bucketName + i);
				System.out.println("Bucket with name " + bucketName + i + " created successfully");
			} else {
				System.out.println("Bucket name " + bucketName + i + " is not available."
						+ " Try again with a different Bucket name.");
			}
			i++;
		}
		System.out.println("==============================================================");
	}

}
