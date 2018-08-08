import grnet.argo.flink.ComputeEngineConfig;
import grnet.argo.flink.ConfigManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.io.FileNotFoundException;

public class ConfigManagerTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Assert that files are present
        Assert.assertNotNull("Test file missing", ConfigManagerTest.class.getResource("/testconf.json"));
    }

    @Test
    public void test_configurationSetup() throws Exception {
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "testconf.json");
        Integer hdfs_port = 9999;
        Integer mongo_port = 9999;
        Boolean ssl_enabled = true;
        Boolean proxy_enabled = true;
        Integer proxy_port = 9999;
        Integer retry_count = 5;
        Integer retry_interval = 5000;

        Assert.assertEquals("ams_host_test", ce_cfg.getAms_host());
        Assert.assertEquals("ams_project_test", ce_cfg.getAms_project());
        Assert.assertEquals("ams_token_test", ce_cfg.getAms_token());
        Assert.assertEquals("ams_pull_sub_test", ce_cfg.getAms_pull_sub());
        Assert.assertEquals("ams_publish_topic_test", ce_cfg.getAms_publish_topic());
        Assert.assertEquals("https://{{ams_host}}/v1/projects/{{ams_project}}/subscriptions/{{ams_pull_sub}}:pull?key={{ams_token}}", ce_cfg.getAms_pull_url());
        Assert.assertEquals("https://{{ams_host}}/v1/projects/{{ams_project}}/topics/{{ams_publish_topic}}:publish?key={{ams_token}}", ce_cfg.getAms_publish_url());
        Assert.assertEquals("mongo_host_test", ce_cfg.getMongo_host());
        Assert.assertEquals("mongo_db_test", ce_cfg.getMongo_db());
        Assert.assertEquals("mongo_col_test", ce_cfg.getMongo_col());
        Assert.assertEquals("hdfs_nn_test", ce_cfg.getHdfs_nn());
        Assert.assertEquals("hdfs_filename_test", ce_cfg.getHdfs_filename());
        Assert.assertEquals("hdfs_user_test", ce_cfg.getHdfs_user());
        Assert.assertEquals("proxy url", ce_cfg.getProxy_url());
        Assert.assertEquals("https", ce_cfg.getProxy_scheme());
        Assert.assertEquals("hdfs://{{hdfs_nn}}:{{hdfs_port}}/user/{{hdfs_user}}/{{hdfs_filename}}", ce_cfg.getHdfs_path());
        Assert.assertEquals(mongo_port, ce_cfg.getMongo_port());
        Assert.assertEquals(hdfs_port, ce_cfg.getHdfs_port());
        Assert.assertEquals(ssl_enabled, ce_cfg.getSsl_enabled());
        Assert.assertEquals(proxy_enabled, ce_cfg.getProxy_enabled());
        Assert.assertEquals(proxy_port, ce_cfg.getProxy_port());
        Assert.assertEquals(retry_count, ce_cfg.getRetry_count());
        Assert.assertEquals(retry_interval, ce_cfg.getRetry_interval());
    }

    @Test
    public void test_composeURL() throws Exception{
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "testconf.json");
        String ams_pull_url = "https://ams_host_test/v1/projects/ams_project_test/subscriptions/ams_pull_sub_test:pull?key=ams_token_test";
        String ams_publish_url = "https://ams_host_test/v1/projects/ams_project_test/topics/ams_publish_topic_test:publish?key=ams_token_test";
        String hdfs_path = "hdfs://hdfs_nn_test:9999/user/hdfs_user_test/hdfs_filename_test";

        Assert.assertEquals(ams_pull_url, ConfigManager.composeURL(ce_cfg.getAms_pull_url(), "\\{\\{.*?\\}\\}", ce_cfg));
        Assert.assertEquals(ams_publish_url, ConfigManager.composeURL(ce_cfg.getAms_publish_url(), "\\{\\{.*?\\}\\}", ce_cfg));
        Assert.assertEquals(hdfs_path, ConfigManager.composeURL(ce_cfg.getHdfs_path(), "\\{\\{.*?\\}\\}", ce_cfg));
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test_configurationSetupWithUndeclaredValueInJsonFile() throws Exception{
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("Null value for field : ams_host. Check the configuration file and make sure you have declared all fields.");
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "missing_field.json");
    }

    @Test
    public void test_configurationSetupWithWrongFilepath() throws Exception{
        thrown.expect(FileNotFoundException.class);
        thrown.expectMessage("wrong_path.json (No such file or directory). File was not found neither in the classpath nor in the filesystem.");
        ConfigManager.configurationSetup(ComputeEngineConfig.class, "wrong_path.json");
    }
}