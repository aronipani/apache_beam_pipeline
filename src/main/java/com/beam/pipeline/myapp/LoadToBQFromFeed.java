package com.beam.pipeline.myapp;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * The {@link PubsubToBigQueryDynamicDestinations} is a streaming pipeline which dynamically routes
 * messages to their output location using an attribute within the Pub/Sub message header. This
 * pipeline requires any tables which will be routed to, to be defined prior to execution.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub subscription must exist prior to pipeline execution.
 *   <li>The BigQuery output tables routed to must be created prior to pipeline execution.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT_ID
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/pubsub-to-bigquery-dynamic-destinations
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.pso.pipeline.PubsubToBigQueryDynamicDestinations \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * --subscription=SUBSCRIPTION \
 * --tableNameAttr=ATTRIBUTE_NAME \
 * --outputTableProject=PROJECT \
 * --outputTableDataset=DATASET \
 * --streaming=true \
 * --triggerMaxHeight=30.0"
 * </pre>
 */
public class LoadToBQFromFeed {

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions {
        @Required
        String getSubscription();
        void setSubscription(String value);

        @Required
        String getOutputTableDataset();
        void setOutputTableDataset(String value);

        @Required
        boolean getStreaming();
        void setStreaming(boolean value);

        @Required
        String getTriggerMaxHeight();
        void setTriggerMaxHeight(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
        //PipelineOptions options = PipelineOptionsFactory.create();
        //DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
/*      System.out.println("Project => " +  options.getMyProject());
        System.out.println("Runner => " +  options.getRunner());
        System.out.println("TriggerMaxHeight => " +  options.getTriggerMaxHeight());

 */
        run(options);

    }

    public static PipelineResult run(Options options) {

        // Create the pipeline
        //PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Retrieve non-serializable parameters
        //String tableNameAttr = options.getTableNameAttr();
        //String tableDataset = options.getOutputTableDataset();
        String triggerMaxHeight = options.getTriggerMaxHeight();
        System.out.println("Pipeline = > " + pipeline);

        // Build & execute pipeline
        pipeline
                .apply(
                        "ReadMessages",
                        PubsubIO.readMessagesWithAttributes().fromTopic(options.getSubscription()))
                .apply(
                        "WriteToBigQuery",
                        BigQueryIO.<PubsubMessage>write()
                                .to(options.getOutputTableDataset())
                                .withFormatFunction(
                                        (PubsubMessage msg) -> convertJsonToTableRow(new String(msg.getPayload()), triggerMaxHeight))
                                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

        return pipeline.run();
    }


    /**
     * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
     * RuntimeException} will be thrown.
     *
     * @param json The JSON string to parse.
     * @return The parsed {@link TableRow} object.
     */
    @VisibleForTesting
    static TableRow convertJsonToTableRow(String json,String triggerMaxHeight) {
        TableRow row;
        // Parse the JSON into a {@link TableRow} object.
        try (InputStream inputStream =
                     new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

            if(Float.parseFloat(row.get("maximum_water_height").toString()) >= Float.parseFloat(triggerMaxHeight))
                row.put("trigger_alert", true);
            else
                row.put("trigger_alert", false);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
        return row;
    }
}