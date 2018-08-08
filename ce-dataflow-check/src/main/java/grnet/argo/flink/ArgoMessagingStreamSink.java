package grnet.argo.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1> ArgoMessagingSink class</h1>
 * <p> Class responsible for providing FLINK with thr ability to sink data into AMS</p>
 */
public class ArgoMessagingStreamSink extends RichSinkFunction<MessageData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArgoMessagingStreamSink.class);

    private ArgoMessagingClient ams_client = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters gp = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.ams_client = new ArgoMessagingClient((ComputeEngineConfig) gp);
        LOGGER.info("ArgoMessagingStreamSink set up is ready.");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.ams_client != null)  {
            this.ams_client.close();
        }
    }

    @Override
    public void invoke(MessageData messageData) {
        String ams_pub_res;
        try {
            ams_pub_res = this.ams_client.doPublishMessage(messageData);
            LOGGER.info("Published back to AMS. " + messageData.toString());
            LOGGER.info("Response after publishing: " + ams_pub_res);
        } catch (Exception e) {
            LOGGER.error("ERROR while trying to publish message to AMS." + e.getMessage());
        }
    }
}
