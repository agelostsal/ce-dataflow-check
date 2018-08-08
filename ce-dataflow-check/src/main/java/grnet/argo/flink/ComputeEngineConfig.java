package grnet.argo.flink;

import org.apache.flink.api.common.ExecutionConfig;

/**
 * <h1> ComputeEngineConfig class</h1>
 * <p> Class for holding configuration information needed for various aspects of the job.
 * It also extends the ExecutionConfig.GlobalJobParameters in order to be accessible in all of FLink's RichFunctions.</p>
 */
public class ComputeEngineConfig extends ExecutionConfig.GlobalJobParameters {

    private String ams_host;
    private String ams_project;
    private String ams_token;
    private String ams_pull_sub;
    private String ams_publish_topic;
    private String ams_pull_url;
    private String ams_publish_url;
    private String mongo_host;
    private Integer mongo_port;
    private String mongo_db;
    private String mongo_col;
    private String hdfs_nn;
    private Integer hdfs_port;
    private String hdfs_filename;
    private String hdfs_user;
    private String hdfs_path;
    private Boolean proxy_enabled;
    private String proxy_scheme;
    private String proxy_url;
    private Integer proxy_port;
    private Boolean ssl_enabled;
    private Integer retry_count;
    private Integer retry_interval;

    public ComputeEngineConfig(String ams_host, String ams_project, String ams_token, String ams_pull_sub, String ams_publish_topic, String ams_pull_url,
                               String ams_publish_url, String mongo_host, Integer mongo_port, String mongo_db, String mongo_col, String hdfs_nn, Integer hdfs_port,
                               String hdfs_filename, String hdfs_user, String hdfs_path, Boolean proxy_enabled, String proxy_scheme,
                               String proxy_url, Integer proxy_port, Boolean ssl_enabled, Integer retry_count, Integer retry_interval) {

        this.ams_host = ams_host;
        this.ams_project = ams_project;
        this.ams_token = ams_token;
        this.ams_pull_sub = ams_pull_sub;
        this.ams_publish_topic = ams_publish_topic;
        this.ams_pull_url = ams_pull_url;
        this.ams_publish_url = ams_publish_url;
        this.mongo_host = mongo_host;
        this.mongo_port = mongo_port;
        this.mongo_db = mongo_db;
        this.mongo_col = mongo_col;
        this.hdfs_nn = hdfs_nn;
        this.hdfs_port = hdfs_port;
        this.hdfs_filename = hdfs_filename;
        this.hdfs_user = hdfs_user;
        this.hdfs_path = hdfs_path;
        this.proxy_enabled = proxy_enabled;
        this.proxy_scheme = proxy_scheme;
        this.proxy_url = proxy_url;
        this.proxy_port = proxy_port;
        this.ssl_enabled = ssl_enabled;
        this.retry_count = retry_count;
        this.retry_interval = retry_interval;

    }

    public String getAms_host() {
        return ams_host;
    }

    public void setAms_host(String ams_host) {
        this.ams_host = ams_host;
    }

    public String getAms_project() {
        return ams_project;
    }

    public void setAms_project(String ams_project) {
        this.ams_project = ams_project;
    }

    public String getAms_token() {
        return ams_token;
    }

    public void setAms_token(String ams_token) {
        this.ams_token = ams_token;
    }

    public String getAms_pull_sub() {
        return ams_pull_sub;
    }

    public void setAms_pull_sub(String ams_pull_sub) {
        this.ams_pull_sub = ams_pull_sub;
    }

    public String getAms_publish_topic() {
        return ams_publish_topic;
    }

    public void setAms_publish_topic(String ams_publish_topic) {
        this.ams_publish_topic = ams_publish_topic;
    }

    public String getAms_pull_url() {
        return ams_pull_url;
    }

    public void setAms_pull_url(String ams_pull_url) {
        this.ams_pull_url = ams_pull_url;
    }

    public String getAms_publish_url() {
        return ams_publish_url;
    }

    public void setAms_publish_url(String ams_publish_url) {
        this.ams_publish_url = ams_publish_url;
    }

    public String getMongo_host() {
        return mongo_host;
    }

    public void setMongo_host(String mongo_host) {
        this.mongo_host = mongo_host;
    }

    public Integer getMongo_port() {
        return mongo_port;
    }

    public void setMongo_port(Integer mongo_port) {
        this.mongo_port = mongo_port;
    }

    public String getMongo_db() {
        return mongo_db;
    }

    public void setMongo_db(String mongo_db) {
        this.mongo_db = mongo_db;
    }

    public String getMongo_col() {
        return mongo_col;
    }

    public void setMongo_col(String mongo_col) {
        this.mongo_col = mongo_col;
    }

    public String getHdfs_nn() {
        return hdfs_nn;
    }

    public void setHdfs_nn(String hdfs_nn) {
        this.hdfs_nn = hdfs_nn;
    }

    public Integer getHdfs_port() {
        return hdfs_port;
    }

    public void setHdfs_port(Integer hdfs_port) {
        this.hdfs_port = hdfs_port;
    }

    public String getHdfs_filename() {
        return hdfs_filename;
    }

    public void setHdfs_filename(String hdfs_filename) {
        this.hdfs_filename = hdfs_filename;
    }

    public String getHdfs_user() {
        return hdfs_user;
    }

    public void setHdfs_user(String hdfs_user) {
        this.hdfs_user = hdfs_user;
    }

    public String getHdfs_path() {
        return hdfs_path;
    }

    public void setHdfs_path(String hdfs_path) {
        this.hdfs_path = hdfs_path;
    }

    public Boolean getProxy_enabled() {
        return proxy_enabled;
    }

    public void setProxy_enabled(Boolean proxy_enabled) {
        this.proxy_enabled = proxy_enabled;
    }

    public String getProxy_scheme() {
        return proxy_scheme;
    }

    public void setProxy_scheme(String proxy_scheme) {
        this.proxy_scheme = proxy_scheme;
    }

    public String getProxy_url() {
        return proxy_url;
    }

    public void setProxy_url(String proxy_url) {
        this.proxy_url = proxy_url;
    }

    public Integer getProxy_port() {
        return proxy_port;
    }

    public void setProxy_port(Integer proxy_port) {
        this.proxy_port = proxy_port;
    }

    public Boolean getSsl_enabled() {
        return ssl_enabled;
    }

    public void setSsl_enabled(Boolean ssl_enabled) {
        this.ssl_enabled = ssl_enabled;
    }

    public Integer getRetry_count() {
        return retry_count;
    }

    public void setRetry_count(Integer retry_count) {
        this.retry_count = retry_count;
    }


    public Integer getRetry_interval() {
        return retry_interval;
    }

    public void setRetry_interval(Integer retry_interval) {
        this.retry_interval = retry_interval;
    }
}