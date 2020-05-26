package com.mycompany.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) throws IOException
    {  
        
        AWSCredentials credentials = new BasicAWSCredentials("AKIAJZDHCBVU4JKIJRSA", "kSOaPe6hGW5l4d9nt1BSsGZ+cjI86GZZSv2jOS6X");
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_2).build();

        String bucketName = "project-2-group-3-bucket-cbpc";
        if (s3client.doesBucketExist(bucketName)) {
                    System.out.println("Bucket name is not available." + "Try again with a different Bucket name.");
                    return;
        }
        
        
        List<Bucket> buckets = s3client.listBuckets();
        for (Bucket bucket : buckets) {
                System.out.println(bucket.getName());
        }

        ObjectListing objectListing = s3client.listObjects("project-2-group-3-bucket-cpbc");
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
                System.out.println(os.getKey());
        }

        S3Object fullObject = null;
        String key = "Input/vgsales-12-4-2019-short.csv";
        System.out.println("Downloading an object");
        fullObject = s3client.getObject(new GetObjectRequest("project-2-group-3-bucket-cpbc", key));
        System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
        System.out.println("Content: ");
        System.out.println(fullObject.getObjectContent());

        //com.amazonaws.services.s3.model.S3Object s3object = s3client.getObject("project-2-group-3-bucket-cpbc",
        //"Input/vgsales-12-4-2019-short.csv");
        //S3ObjectInputStream inputStream = s3object.getObjectContent();

        BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null && count < 10){
            System.out.println(line);
            count++;
        } 
   
/*
SparkSession spark = new SparkSession.Builder().appName("APP").master("local[*]").getOrCreate();
spark.sparkContext().setLogLevel("WARN");
spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3-us-east-2.amazonaws.com");
spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "AKIAJZDHCBVU4JKIJRSA");
spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "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");
//spark.sparkContext().hadoopConfiguration().addResource("conf.xml");
Dataset<Row> ds = spark.read().option("header", true)
 .csv("s3a://project-2-group-3-bucket-cpbc/Input/vgsales-12-4-2019-short.csv").cache();
ds.printSchema();*/
    }
}
