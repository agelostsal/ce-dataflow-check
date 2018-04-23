package grnet.argo.flink;

import com.google.gson.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1>ArgoMessagingStreamSource class</h1>
 * <p>Class that provides FLINK the ability to use AMS as a source.</p>
 */
public class ArgoMessagingStreamSource extends RichSourceFunction<MessageData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArgoMessagingStreamSource.class);

    private volatile boolean isRunning = true;
    private ArgoMessagingClient ams_client = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters gp = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.ams_client= new ArgoMessagingClient((ComputeEngineConfig)gp);
        LOGGER.info("ArgoMessagingStreamSource set up is ready.");
    }

    @Override
    public void run(SourceContext<MessageData> sourceContext) throws Exception {
        String previous_msg = "";
        String current_msg ;
        while (this.isRunning) {
            Thread.sleep(5000);
            current_msg = this.ams_client.doPullMessage();
            if (!current_msg.equals(previous_msg)) {
                previous_msg = current_msg;
                ArgoMessagingResponse ams_resp = new GsonBuilder().serializeNulls().create().fromJson(current_msg, ArgoMessagingResponse.class);
                if (ams_resp.getReceivedMessages().length != 0) {
                    MessageData msg_data = new Gson().fromJson(ams_resp.getReceivedMessages()[0].getMessage().getData(), MessageData.class);
                    LOGGER.info("Collecting new message: " + current_msg);
                    sourceContext.collect(msg_data);
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}