FROM gcr.io/dataflow-templates-base/java11-template-launcher-base:latest

# Define the Java command options required by Dataflow Flex Templates.
ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.telus.mediation.dataflow.ExtractGzipPubSubToBQ"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="/template/cio-mediation-dataflow.jar"

# Make sure to package as an uber-jar including all dependencies.
COPY target/cio-mediation-datahub-load-bundled-0.0.1.jar ${FLEX_TEMPLATE_JAVA_CLASSPATH}