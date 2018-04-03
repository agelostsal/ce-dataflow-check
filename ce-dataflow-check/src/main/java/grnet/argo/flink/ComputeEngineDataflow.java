package grnet.argo.flink;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.util.Date;

public class ComputeEngineDataflow {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputeEngineDataflow.class);

    public static void main(String[] args) throws Exception {


            // default configuration file, found in the project's resources folder
             String conf_path = "conf.json";

            final ParameterTool params = ParameterTool.fromArgs(args);

            // check if the user has specified a location for the configuration file
            if(params.get("config") != null) {
                conf_path = params.get("config");
            }

            final ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, conf_path);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.getConfig().setGlobalJobParameters(ce_cfg);

            DataStream<MessageData> ds = env.addSource(new ArgoMessagingStreamSource());
            ds.flatMap(new MongoRichFlatMap())
                    .flatMap(new HdfsRichFlatMap())
                    .addSink(new ArgoMessagingStreamSink());
            ds.print();
            env.execute("Compute Engine Dataflow Check");
    }

    public static class MongoRichFlatMap extends RichFlatMapFunction<MessageData, MessageData> {

        private  final Logger LOGGER = LoggerFactory.getLogger(MongoRichFlatMap.class);

        private ComputeEngineConfig ce_cfg;
        private MongoClient mongoClient;
        private MongoDatabase db;
        private Gson gson;

        @Override
        public void open(Configuration parameters) throws  Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters gp = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.ce_cfg = (ComputeEngineConfig) gp;
            this.gson = new Gson();
            this.mongoClient = new MongoClient(ce_cfg.getMongo_host(), ce_cfg.getMongo_port());
            this.db = mongoClient.getDatabase(ce_cfg.getMongo_db());
        }

        @Override
        public void flatMap(MessageData messageData, Collector<MessageData> collector) throws Exception {
            try {
                LOGGER.info("Opening Connection to mongo...");
                // write the message to mongo and then try to retrieve it
                this.db.getCollection("check_msg").insertOne(new Document().append("dataflow_msg", this.gson.toJson(messageData)));
                if (this.db.getCollection("check_msg").findOneAndDelete(Filters.eq("dataflow_msg", this.gson.toJson(messageData))) == null){
                    throw new NullPointerException("Message was not written in mongo.");
                }
            } catch (Exception e) {
                messageData.setError(new DataflowError("Mongo", e.getMessage()));
                LOGGER.error(e.getMessage());
            } finally {
                collector.collect((MessageData)messageData.clone());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.mongoClient.close();
        }
    }

    public static class HdfsRichFlatMap extends RichFlatMapFunction<MessageData, MessageData> {

        private static final Logger LOGGER = LoggerFactory.getLogger(HdfsRichFlatMap.class);

        ComputeEngineConfig ce_cfg;
        Gson gson;
        org.apache.hadoop.conf.Configuration hadoop_cfg;
        FileSystem fs ;
        Path hdfs_path;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters gp = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.ce_cfg = (ComputeEngineConfig) gp;
            this.gson = new Gson();
            // Init HDFS File System Object
            this.hadoop_cfg = new org.apache.hadoop.conf.Configuration();
            // Set FileSystem URI
            this.hdfs_path = new Path(ConfigManager.composeURL(this.ce_cfg.getHdfs_path(), "\\{\\{.*?\\}\\}", this.ce_cfg));
            this.hadoop_cfg.set("fs.defaultFS", this.hdfs_path.toString());
            this.hadoop_cfg.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            this.hadoop_cfg.set("fs.file.impl", org.apache.hadoop.fs.FileSystem.class.getName());
            this.fs = FileSystem.get(URI.create(this.hdfs_path.toString()), this.hadoop_cfg);
        }

        @Override
        public void flatMap(MessageData messageData, Collector<MessageData> collector) throws Exception {
            FSDataOutputStream output_stream = null;
            FSDataInputStream input_stream = null;
            try {
                // if the file already exists, delete it,
                if (this.fs.exists(this.hdfs_path)) {
                    this.fs.delete(this.hdfs_path, true);
                }
                // remove spaces and :from the date
                String date_now = new Date().toString().replaceAll("\\s|:", "-");
                // write the message, accompanied by date, to an hdfs file
                output_stream = this.fs.create(this.hdfs_path);
                output_stream.writeBytes(this.gson.toJson(messageData) + " @ " + date_now);
                output_stream.close();
                // retrieve the written message
                input_stream = this.fs.open(this.hdfs_path);
                String out = IOUtils.toString(input_stream);
                input_stream.close();
                if (!out.split(" @ ")[0].equals(this.gson.toJson(messageData))) {
                    throw new Exception("Reading the hdfs file didn't return the expected content");
                }
            } catch (Exception e){
                messageData.setError(new DataflowError("HDFS", e.getMessage()));
                LOGGER.error(e.getMessage());
            } finally {
                IOUtils.closeQuietly(output_stream);
                IOUtils.closeQuietly(input_stream);
                collector.collect((MessageData)messageData.clone());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.fs.close();
        }
    }
}
