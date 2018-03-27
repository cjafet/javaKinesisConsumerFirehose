package com.muv.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
//import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
//import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
//import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
//import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
//import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.*;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Firehose {

    public void sendToFirehose(String data, String dsn) {
        String dados = data + "\n";

        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();

        AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentialsProvider.getCredentials()))
                .withRegion(Regions.US_EAST_1)
                .build();

        Record record = new Record();

        record.setData(ByteBuffer.wrap(dados.getBytes(StandardCharsets.UTF_8)));

        PutRecordRequest putRecordRequest = new PutRecordRequest()

                .withDeliveryStreamName(dsn)

                .withRecord(record);

        putRecordRequest.setRecord(record);

        PutRecordResult result = firehoseClient.putRecord(putRecordRequest);

        System.out.println("Result Inserted with ID: "+result.getRecordId());
    }
}
