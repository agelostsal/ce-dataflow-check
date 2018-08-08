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
    private transient Object rateLck; // lock for waiting to establish rate


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters gp = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.ams_client= new ArgoMessagingClient((ComputeEngineConfig)gp);
        // init rate lock
        rateLck = new Object();
        LOGGER.info("ArgoMessagingStreamSource set up is ready.");
    }

    @Override
    public void run(SourceContext<MessageData> sourceContext) throws Exception {
        String previous_msg = "";
        String current_msg ;
        while (this.isRunning) {

            synchronized (rateLck) {
                rateLck.wait(10000);
            }

            LOGGER.info("Trying to pull new messages from AMS");
            try {
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
            } catch (Exception e) {
                LOGGER.error("ERROR while trying to pull messages from AMS." + e.getMessage());
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void close() throws Exception{

        if (this.ams_client != null) {
            this.ams_client.close();
        }

        synchronized (rateLck) {
            rateLck.notify();
        }
    }
}