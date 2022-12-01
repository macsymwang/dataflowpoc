package com.telus.mediation.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import com.google.common.io.ByteStreams;
import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import com.telus.mediation.dataflow.template.PrintMessageFn;
import com.telus.mediation.dataflow.template.ProcessOptions;
import com.telus.mediation.dataflow.util.LitheString;

public class ExtractGzipPubSubToBQ {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractGzipPubSubToBQ.class);
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
                PubsubIO.readMessages().fromSubscription(options.getInputSubscription()))
                .apply("Uncompress Data From Message", ParDo.of(new UncompressMessageToData()))
                .apply("Extract Data From Message", ParDo.of(new ConvertStringMessageToData()))
                .apply("Print Out Data", ParDo.of(new PrintMessageFn()))
                .apply("Convert to TableRow", ParDo.of(new ConvertDataToTableRow()))
                .apply("Write into BigQuery",
                        BigQueryIO.writeTableRows()
                                .withJsonSchema(getSchemaFromGCS(options.getJSONPath())) // withSchema(ConvertDataToTableRow.getSchema())
                                .to(options.getOutputTable())
                                .withoutValidation()
                                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                                .withTriggeringFrequency(Duration.standardSeconds(5))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }

    static class ConvertMessageToData extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            String textStr[] = element.split("\\r?\\n");
            for (String splitString : textStr) {
                receiver.output(splitString);
            }
        }
    }

    static class UncompressMessageToData extends DoFn<PubsubMessage, String> {
        @ProcessElement
        public void processElement(@Element PubsubMessage element, OutputReceiver<String> receiver) {
            byte textStr[] = element.getPayload();
            String uncompressed = LitheString.unzip(textStr);
            receiver.output(uncompressed);
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

    /**
     * Method to read a BigQuery schema file from GCS and return the file contents
     * as a string.
     *
     * @param gcsPath Path string for the schema file in GCS.
     * @return File contents as a string.
     */
    private static ValueProvider<String> getSchemaFromGCS(ValueProvider<String> gcsPath) {
        return NestedValueProvider.of(
                gcsPath,
                new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        ResourceId sourceResourceId = FileSystems.matchNewResource(input, false);

                        String schema;
                        try (ReadableByteChannel rbc = FileSystems.open(sourceResourceId)) {
                            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                                try (WritableByteChannel wbc = Channels.newChannel(baos)) {
                                    ByteStreams.copy(rbc, wbc);
                                    schema = baos.toString(StandardCharsets.UTF_8.name());
                                    LOG.info("Extracted schema: " + schema);
                                }
                            }
                        } catch (IOException e) {
                            LOG.error("Error extracting schema: " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return schema;
                    }
                });
    }

}
