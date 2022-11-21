
package com.telus.mediation.dataflow;

import java.lang.Double; 

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class ExtractDetails {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        
        runProductDetails(options);
    }
    
    static void runProductDetails(PipelineOptions options) {
        
        Pipeline p = Pipeline.create(options);
    
        p.apply("ReadLines", TextIO.read().from("gs://loony-learn/input_data/Sales_April_2022.csv"))
            .apply("ExtractSalesDetails", ParDo.of(new ExtractSalesDetailsFn()))
            .apply("WriteSalesDetails", TextIO.write().to("gs://loony-learn/output_data/get_sales_details"));

        p.run().waitUntilFinish();
    }
    
    static class ExtractSalesDetailsFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
        String[] field = element.split(",");
        String productDetails = field[1] + "," + 
            Double.toString(Integer.parseInt(field[2]) * Double.parseDouble(field[3])) + "," +  field[4];
        receiver.output(productDetails);
        }
    }
}