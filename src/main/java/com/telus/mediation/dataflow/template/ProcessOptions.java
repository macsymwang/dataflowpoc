package com.telus.mediation.dataflow.template;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;

public interface ProcessOptions extends StreamingOptions {

    @Description("Subscription name for input Pub/Sub")
    String getInputSubscription();

    void setInputSubscription(String value);

}
