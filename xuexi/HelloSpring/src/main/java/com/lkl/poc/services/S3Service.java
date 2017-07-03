package com.lkl.poc.services;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Created by duoduo.liu on 2017-07-02.
 */
public class S3Service {
    private String s3AccessKey;

    private String s3SecretKey;

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    public void upload(String s3bucket, String s3TargetFilePath, String text) {
        AmazonS3 s3client = getS3Client();

        try {
            InputStream stream = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
            ObjectMetadata omd = new ObjectMetadata();
            omd.setContentLength(text.length());

            s3client.putObject(new PutObjectRequest(s3bucket, s3TargetFilePath, stream, omd));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private AmazonS3 getS3Client()
    {
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(this.s3AccessKey, this.s3SecretKey);
        System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true");
        AmazonS3 client = new AmazonS3Client(basicAWSCredentials);
        Region region = Region.getRegion(Regions.US_EAST_1);
        client.setRegion(region);

        return client;
    }
}
