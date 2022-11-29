package com.telus.mediation.dataflow.template;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;

public interface ProcessOptions extends StreamingOptions {
    @Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
    @Default.String("cio-mediation-springdf-lab-3f:sample_ds.order_details")
    String getOutputTable();

    void setOutputTable(String s);

    @Description("Subscription name for input Pub/Sub")
    String getInputSubscription();

    void setInputSubscription(String value);

}
