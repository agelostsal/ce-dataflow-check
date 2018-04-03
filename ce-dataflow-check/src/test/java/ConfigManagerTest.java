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
        Assert.assertNotNull("Test file missing", ConfigManagerTest.class.getResource("/conf.template"));
    }

    @Test
    public void test_configurationSetup() throws Exception {
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "conf.template");
        Integer hdfs_port = 9999;
        Integer mongo_port = 9999;
        Boolean ssl_enabled = true;
        Boolean proxy_enabled = true;
        Integer proxy_port = 9999;

        Assert.assertEquals(ce_cfg.getAms_host(), "ams_host_test");
        Assert.assertEquals(ce_cfg.getAms_project(), "ams_project_test");
        Assert.assertEquals(ce_cfg.getAms_token(), "ams_token_test");
        Assert.assertEquals(ce_cfg.getAms_pull_sub(), "ams_pull_sub_test");
        Assert.assertEquals(ce_cfg.getAms_publish_topic(), "ams_publish_topic_test");
        Assert.assertEquals(ce_cfg.getAms_pull_url(), "https://{{ams_host}}/v1/projects/{{ams_project}}/subscriptions/{{ams_pull_sub}}:pull?key={{ams_token}}");
        Assert.assertEquals(ce_cfg.getAms_publish_url(), "https://{{ams_host}}/v1/projects/{{ams_project}}/topics/{{ams_publish_topic}}:publish?key={{ams_token}}");
        Assert.assertEquals(ce_cfg.getMongo_host(), "mongo_host_test");
        Assert.assertEquals(ce_cfg.getMongo_port(), mongo_port);
        Assert.assertEquals(ce_cfg.getMongo_db(), "mongo_db_test");
        Assert.assertEquals(ce_cfg.getMongo_col(), "mongo_col_test");
        Assert.assertEquals(ce_cfg.getHdfs_nn(), "hdfs_nn_test");
        Assert.assertEquals(ce_cfg.getHdfs_port(), hdfs_port);
        Assert.assertEquals(ce_cfg.getHdfs_filename(), "hdfs_filename_test");
        Assert.assertEquals(ce_cfg.getHdfs_user(), "hdfs_user_test");
        Assert.assertEquals(ce_cfg.getHdfs_path(), "hdfs://{{hdfs_nn}}:{{hdfs_port}}/user/{{hdfs_user}}/{{hdfs_filename}}");
        Assert.assertEquals(ce_cfg.getSsl_enabled(), ssl_enabled);
        Assert.assertEquals(ce_cfg.getProxy_enabled(), proxy_enabled);
        Assert.assertEquals(ce_cfg.getProxy_scheme(), "https");
        Assert.assertEquals(ce_cfg.getProxy_url(), "proxy url");
        Assert.assertEquals(ce_cfg.getProxy_port(), proxy_port);

    }

    @Test
    public void test_composeURL() throws Exception{
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "conf.template");
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
        ComputeEngineConfig ce_cfg = ConfigManager.configurationSetup(ComputeEngineConfig.class, "wrong_path.json");
    }
}