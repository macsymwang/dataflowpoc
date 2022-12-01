package com.telus.mediation.dataflow.template;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface ProcessOptions extends DataflowPipelineOptions {
    @Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
    @Default.String("cio-mediation-springdf-lab-3f:sample_ds.order_details")
    String getOutputTable();

    void setOutputTable(String s);

    @Description("Subscription name for input Pub/Sub")
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description("Cloud Storage location of your BigQuery schema file, described as a JSON")
    // @Default.String("gs://loony-learn/config_files/schema.json")
    void setJSONPath(ValueProvider<String> value);

    ValueProvider<String> getJSONPath();

}
