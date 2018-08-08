package grnet.argo.flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h1> ArgoMessagingClient class</h1>
 * <p> A class that provides us with the ability to interact with the Argo messaging service.</p>
 */
public class ArgoMessagingClient {

    static Logger LOGGER = LoggerFactory.getLogger(ArgoMessagingClient.class);

    private CloseableHttpClient httpClient = null;
    private ComputeEngineConfig ce_cfg = null;
    private RequestConfig req_cfg =  null;

    public ArgoMessagingClient(ComputeEngineConfig ce_cfg) throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException{
        this.ce_cfg = ce_cfg;
        BuildClientConfig();
        BuildHttpClient();
    }

    /**
     *  Configure the httpclient
     */
    private void BuildHttpClient() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException{

        // check if ssl is enabled
        SSLConnectionSocketFactory sslsf = null;

        if (!this.ce_cfg.getSsl_enabled()) {
            // Create ssl context
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            sslsf = new SSLConnectionSocketFactory(builder.build());
        }

        // socket config
        SocketConfig socketCfg = SocketConfig.custom().
                setSoTimeout(30000).
                build();

        // build the client
        this.httpClient = HttpClients.custom().
                setSSLSocketFactory(sslsf).
                setDefaultRequestConfig(this.req_cfg).
                setDefaultSocketConfig(socketCfg).
                setRetryHandler(new AmsHttpRequestRetryHandler(this.ce_cfg.getRetry_count(), this.ce_cfg.getRetry_interval())).
                build();
    }

    /**
     * Create a configuration for using http proxy on each request
     */
    private void BuildClientConfig(){

        RequestConfig.Builder client_builder = RequestConfig.custom();

        // check if there proxy is enabled
        if (this.ce_cfg.getProxy_enabled()) {
            HttpHost proxy = new HttpHost(this.ce_cfg.getProxy_url(), this.ce_cfg.getProxy_port(), this.ce_cfg.getProxy_scheme());
            client_builder.setProxy(proxy);
        }

        // set timeout policy
        client_builder.setConnectTimeout(30000);
        client_builder.setConnectionRequestTimeout(30000);

        // build the config
        this.req_cfg = client_builder.build();

    }

    /**
     * Method that consume a message from an Ams subscription, and returns its contents.
     */
    public String doPullMessage() throws Exception {
        String url = ConfigManager.composeURL(this.ce_cfg.getAms_pull_url(), "\\{\\{.*?\\}\\}", this.ce_cfg);
        StringEntity postBody = new StringEntity(
                "{\"maxMessages\":\"1\", \"returnImmediately\":\"true\"}");
        postBody.setContentType("application/json");
        return this.doPost(url, postBody);
    }

    /**
     * Method that publishes a message to a topic in AMS.
     */
    public  String doPublishMessage(MessageData msg_data) throws Exception {
        Gson gson = new GsonBuilder().serializeNulls().create();
        String url = ConfigManager.composeURL(this.ce_cfg.getAms_publish_url(), "\\{\\{.*?\\}\\}", this.ce_cfg);
        ArgoPublishRequestStructure[] req_structure = new ArgoPublishRequestStructure[1] ;
        req_structure[0] = new ArgoPublishRequestStructure(null, gson.toJson(msg_data));
        StringEntity postBody = new StringEntity(gson.toJson(new ArgoPublishRequest(req_structure)));
        System.out.println(gson.toJson(msg_data));
        postBody.setContentType("application/json");
        return this.doPost(url, postBody);
    }

    /**
     * Method that executes a POST requests against the given url and returns the content of the response as string.
     * @param url for the request
     * @param content Body of the request
     * @return Response body
     * @throws IOException
     */
    private String doPost(String url, StringEntity content) throws IOException{

        // Set up a post request
        HttpPost post_req = new HttpPost(url);
        post_req.setEntity(content);
        CloseableHttpResponse response = this.httpClient.execute(post_req);
        HttpEntity entity = response.getEntity();
        // Read the response
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
        StringBuilder current_msg = new StringBuilder();
        while ((line = br.readLine()) != null) {
            current_msg.append(line);
        }
        // Make sure that the interaction with the service has closed
        EntityUtils.consume(entity);
        response.close();
        return current_msg.toString();
        }

    /**
     * Close AMS  client
     */
    public void close() throws IOException {
        this.httpClient.close();
    }

    class AmsHttpRequestRetryHandler implements HttpRequestRetryHandler {

        private int retryCount;
        private int retryInterval;

        public AmsHttpRequestRetryHandler(int retryCount, int retryInterval) {
            this.retryCount = retryCount;
            this.retryInterval = retryInterval;
        }

        @Override
        public boolean retryRequest(IOException e, int retryCount, HttpContext httpContext) {
            LOGGER.error("Retrying to connect to AMS. Attempt: " + String.valueOf(retryCount));
            synchronized (this) {
                try {
                    wait(this.retryInterval);
                } catch(Exception exp) {
                    LOGGER.error(exp.getMessage());
                }
            }
            return retryCount < this.retryCount;
        }
    }

    class ArgoPublishRequest {
        ArgoPublishRequestStructure[] messages;

        public ArgoPublishRequest(ArgoPublishRequestStructure[] messages) {
            this.messages = messages;
        }
    }

    class ArgoPublishRequestStructure {
        Attribute attributes;
        String data;

         public ArgoPublishRequestStructure(Attribute attributes, String data) {
            this.attributes = attributes;
            this.data = data;
        }
    }
    class Attribute{}
}



