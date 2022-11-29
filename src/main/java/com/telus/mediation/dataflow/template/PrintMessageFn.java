package com.telus.mediation.dataflow.template;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintMessageFn extends DoFn<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(PrintMessageFn.class);

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
        String str = element.toString();
        LOG.info("Processing message:" + str);
        receiver.output(str);
    }
}
