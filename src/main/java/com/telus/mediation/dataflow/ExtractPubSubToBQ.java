package com.telus.mediation.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import com.telus.mediation.dataflow.template.PrintMessageFn;
import com.telus.mediation.dataflow.template.ProcessOptions;

public class ExtractPubSubToBQ {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractPubSubToBQ.class);
    private static final String projectId = "cio-mediation-springdf-lab-3f";
    private static final String datasetId = "sample_ds";
    private static final String tableId = "order_details";

    public static void main(String[] args) {

        TableReference tableRef = new TableReference();
        // Replace this with your own GCP project id
        tableRef.setProjectId(projectId);
        tableRef.setDatasetId(datasetId);
        tableRef.setTableId(tableId);

        ProcessOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ProcessOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("ReadPubSubSubscription",
                PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
                .apply("Extract Data From Message", ParDo.of(new ConvertStringMessageToData()))
                .apply("Print Out Data", ParDo.of(new PrintMessageFn()))
                .apply("Convert to TableRow", ParDo.of(new ConvertDataToTableRow()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows().to(options.getOutputTable())
                                .withSchema(ConvertDataToTableRow.getSchema())
                                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                                .withTriggeringFrequency(Duration.standardSeconds(5))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class ConvertMessageToData extends DoFn<PubsubMessage, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String textStr[] = element.split("\\r?\\n");
            for (String splitString : textStr) {
                receiver.output(splitString);
            }
        }
    }

    static class ConvertStringMessageToData extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String textStr[] = element.split("\\r?\\n");
            for (String splitString : textStr) {
                receiver.output(splitString);
            }
        }
    }

    static class ConvertDataToTableRow extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<TableRow> receiver) {
            TableRow row = new TableRow();
            JSONObject json = new JSONObject(element);
            row.set("Order_ID", json.getString("OrderID"));
            row.set("Amount", json.getInt("Amount"));
            row.set("Profit", json.getInt("Profit"));
            row.set("Quantity", json.getInt("Quantity"));
            row.set("Category", json.getString("Category"));
            row.set("Sub_Category", json.getString("SubCategory"));
            try {
                LOG.info("TableRow:" + row.toPrettyString());
            } catch (IOException e) {
                LOG.error("Get error when print tablerow:" + e.getMessage());
                e.printStackTrace();
            }
            receiver.output(row);
        }

        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            // Currently store all values as String
            fields.add(new TableFieldSchema().setName("Order_ID").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Amount").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Profit").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Quantity").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Category").setType("STRING"));
            fields.add(new TableFieldSchema().setName("Sub_Category").setType("STRING"));
            return new TableSchema().setFields(fields);
        }
    }
}
