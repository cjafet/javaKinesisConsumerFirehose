package com.muv.kinesis;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class Processor implements IRecordProcessor {

    private static AmazonKinesisFirehose firehoseClient;

    @Override
    public void initialize(InitializationInput initializationInput) {

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {

        for (Record record : processRecordsInput.getRecords()) {
            String partKey = record.getPartitionKey();
            ByteBuffer data = record.getData();
            String jsonData = new String(data.array(), StandardCharsets.UTF_8);
            String seq = record.getSequenceNumber();
            System.out.println(jsonData);

            Firehose firehose = new Firehose();
            firehose.sendToFirehose(jsonData, "firehose");
            firehose.sendToFirehose(jsonData, "firehose-es");
            firehose.sendToFirehose(jsonData, "firehose-red");

        }
    }



    @Override
    public void shutdown(ShutdownInput shutdownInput) {

    }
}
