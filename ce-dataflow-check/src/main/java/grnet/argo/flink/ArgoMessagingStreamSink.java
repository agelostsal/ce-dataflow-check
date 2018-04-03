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
    public void invoke(MessageData messageData) throws Exception {
       this.ams_client.doPublishMessage(messageData);
    }
}
