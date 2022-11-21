package com.telus.mediation.dataflow;

import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.Duration;
import com.google.gson.Gson;
import com.telus.mediation.dataflow.module.OrderDetails;

public class ExtractFileOrderDetails {
    private static final Logger logger = LoggerFactory.getLogger(ExtractFileOrderDetails.class);

    private static final String inputFile = "gs://loony-learn/input_data/*.json";
    private static final String outputFile = "gs://loony-learn/output_data/get_sales_details";
    private static final String projectId = "cio-mediation-springdf-lab-3f";
    private static final String datasetId = "sample_ds";
    private static final String tableId = "order_details";
    private static final String bucketName = "loony-learn";
    static Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build()
            .getService();

    public static void main(String[] args) {
        // Start dataflow
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        runProductDetails(options);
    }

    static void runProductDetails(PipelineOptions options) {
        boolean isStreaming = false;
        TableReference tableRef = new TableReference();
        // Replace this with your own GCP project id
        tableRef.setProjectId(projectId);
        tableRef.setDatasetId(datasetId);
        tableRef.setTableId(tableId);

        Pipeline p = Pipeline.create(options);
        PCollection<ReadableFile> files = p
                .apply("Watch Files",
                        FileIO.match().filepattern(inputFile))
                .apply(FileIO.readMatches());
        // Watch.Growth.<String>never()

        PCollection<String> filelist = files
                .apply("Collect file names",
                        ParDo.of(
                                new DoFn<FileIO.ReadableFile, String>() {
                                    @ProcessElement
                                    public void process(@Element ReadableFile file, OutputReceiver<String> out) {
                                        // We can now access the file and its metadata.
                                        logger.info("New File Name is {} ", "gs://" + bucketName + "/" + file);
                                        out.output(file.getMetadata().resourceId().toString());
                                    }
                                }));

        PCollection<String> jsons = filelist.apply("Read Files", TextIO.readAll());

        PCollection<OrderDetails> orderDetails = jsons.apply("Parse Json to Beam Rows",
                ParDo.of(new ConvertSalesDetailsFn()));

        orderDetails.apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                        : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        files.apply(Wait.on(orderDetails)).apply("Archive Files",
                ParDo.of(
                        new DoFn<FileIO.ReadableFile, String>() {
                            @ProcessElement
                            public void process(@Element FileIO.ReadableFile file, OutputReceiver<String> out) {
                                // We can now access the file and its metadata.
                                logger.info("File Metadata resourceId is {} ", file.getMetadata().resourceId());
                                String fileName = StringUtils.removeStart(
                                        file.getMetadata().resourceId().toString(),
                                        "gs://" + bucketName + "/");
                                logger.info("Object name is " + fileName);
                                BlobId source = BlobId.of(bucketName, fileName);
                                BlobId target = BlobId.of(
                                        bucketName, StringUtils.replaceOnce(fileName, "input_data", "archive"));
                                storage.copy(
                                        Storage.CopyRequest.newBuilder().setSource(source)
                                                .setTarget(target).build());
                                storage.get(source).delete();
                                out.output(StringUtils.replaceOnce(fileName, "input_data", "archive"));
                            }
                        }));

        p.run().waitUntilFinish();
    }

    static class ConvertSalesDetailsFn extends DoFn<String, OrderDetails> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<OrderDetails> receiver) {
            Gson gson = new Gson();
            // logger.info("Start processing " + element);
            OrderDetails orderDetails = gson.fromJson(element, OrderDetails.class);
            receiver.output(orderDetails);
        }
    }

    public static class FormatForBigquery extends DoFn<OrderDetails, TableRow> {

        @ProcessElement
        public void processElement(@Element OrderDetails element, OutputReceiver<TableRow> receiver) {
            TableRow row = new TableRow();
            row.set("Order_ID", element.getOrderID());
            row.set("Amount", element.getAmount());
            row.set("Profit", element.getProfit());
            row.set("Quantity", element.getQuantity());
            row.set("Category", element.getCategory());
            row.set("Sub_Category", element.getSubCategory());
            receiver.output(row);
        }

        /** Defines the BigQuery schema used for the output. */
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
