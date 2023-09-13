package com.sankuai.inf.leaf.snowflake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sankuai.inf.leaf.common.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.CreateMode;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SnowflakeRedisHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRedisHolder.class);
    private String zk_AddressNode = null;//保存自身的key  ip:port-000000001
    private String listenAddress = null;//保存自身的key ip:port
    private int workerID;
    private static final String PREFIX_ZK_PATH = "/snowflake/" + PropertyFactory.getProperties().getProperty("leaf.name");
    private static final String PROP_PATH = System.getProperty("java.io.tmpdir") + File.separator + PropertyFactory.getProperties().getProperty("leaf.name") + "/leafconf/{port}/workerID.properties";
    private static final String PATH_FOREVER = PREFIX_ZK_PATH + "/forever";//保存所有数据持久的节点
    private String ip;
    private int port;
//    private String connectionString; zk address
    private String redisIp;
    private Integer redisPort;
    private String redisPassword;
    private Jedis jedis;
    private long lastUpdateTime;

    public SnowflakeRedisHolder(String ip, int port, String redisIp,Integer redisPort,String redisPassword) {
        this.ip = ip;
        this.port = port;
        this.listenAddress = ip + ":" + port;

        this.redisIp = redisIp;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;

        Jedis jedis = new Jedis(redisIp, redisPort);

        if (redisPassword != null && !"".equals(redisPassword)) {
            jedis.auth(redisPassword);
        }
        this.jedis = jedis;
    }

    public boolean init() {
        try {
            Properties properties = PropertyFactory.getProperties();

            String redisKeyCurrent = properties.getProperty("leaf.redis.key.current");
            String redisKeyNodes = properties.getProperty("leaf.redis.key.nodes");

            Set<String> nodesWorkIds = getNodesWorkerId(redisKeyNodes);

            int currentWorkerId;
            do {

                //获取workerId
                Long incr = jedis.incr(redisKeyCurrent);
                currentWorkerId = (int)(incr % 1024);

            }while (nodesWorkIds.contains(String.valueOf(currentWorkerId)));

            workerID = currentWorkerId;
            //定时上报本机时间给forever节点
            ScheduledUploadData(jedis,redisKeyNodes);

            return true;

        } catch (Exception e) {
            LOGGER.error("Start node ERROR {}", e);
            return false;
        }
    }

    private Set<String> getNodesWorkerId(String heartbeatKey) {
        Set<String> nodesWorkIds=new HashSet<>();

        final int keyLength = heartbeatKey.split(":").length;
        Set<String> keys = jedis.keys(heartbeatKey+":*");
        for (String key : keys) {
            String[] split = key.split(":");
            String nodeWorkId = split[keyLength + 1];
            nodesWorkIds.add(nodeWorkId);
        }
        return nodesWorkIds;
    }


    private void ScheduledUploadData(final Jedis jedis, final String heartbeatKey) {
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "schedule-upload-time");
                thread.setDaemon(true);
                return thread;
            }
        }).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                updateNewData(jedis,workerID,heartbeatKey);
            }
        }, 1L, 3L, TimeUnit.SECONDS);//每3s上报数据

    }

    private boolean checkInitTimeStamp(CuratorFramework curator, String zk_AddressNode) throws Exception {
        byte[] bytes = curator.getData().forPath(zk_AddressNode);
        Endpoint endPoint = deBuildData(new String(bytes));
        //该节点的时间不能小于最后一次上报的时间
        return !(endPoint.getTimestamp() > System.currentTimeMillis());
    }

    /**
     * 创建持久顺序节点 ,并把节点数据放入 value
     *
     * @param curator
     * @return
     * @throws Exception
     */
    private String createNode(CuratorFramework curator) throws Exception {
        try {
            return curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(PATH_FOREVER + "/" + listenAddress + "-", buildData().getBytes());
        } catch (Exception e) {
            LOGGER.error("create node error msg {} ", e.getMessage());
            throw e;
        }
    }

    private void updateNewData(Jedis jedis, int workerID, String heartbeatKey) {
        try {
            if (System.currentTimeMillis() < lastUpdateTime) {
                return;
            }
            String key = heartbeatKey+":"+workerID;
            jedis.set(key,buildData());
            jedis.expire(key,30L);
            lastUpdateTime = System.currentTimeMillis();
        } catch (Exception e) {
            LOGGER.info("update init data error workId is {} error is {}", workerID, e);
        }
    }

    /**
     * 构建需要上传的数据
     *
     * @return
     */
    private String buildData() throws JsonProcessingException {
        Endpoint endpoint = new Endpoint(ip, String.valueOf(port), System.currentTimeMillis());
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(endpoint);
        return json;
    }

    private Endpoint deBuildData(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Endpoint endpoint = mapper.readValue(json, Endpoint.class);
        return endpoint;
    }

//    /**
//     * 在节点文件系统上缓存一个workid值,zk失效,机器重启时保证能够正常启动
//     *
//     * @param workerID
//     */
//    private void updateLocalWorkerID(int workerID) {
//        File leafConfFile = new File(PROP_PATH.replace("{port}", port));
//        boolean exists = leafConfFile.exists();
//        LOGGER.info("file exists status is {}", exists);
//        if (exists) {
//            try {
//                FileUtils.writeStringToFile(leafConfFile, "workerID=" + workerID, false);
//                LOGGER.info("update file cache workerID is {}", workerID);
//            } catch (IOException e) {
//                LOGGER.error("update file cache error ", e);
//            }
//        } else {
//            //不存在文件,父目录页肯定不存在
//            try {
//                boolean mkdirs = leafConfFile.getParentFile().mkdirs();
//                LOGGER.info("init local file cache create parent dis status is {}, worker id is {}", mkdirs, workerID);
//                if (mkdirs) {
//                    if (leafConfFile.createNewFile()) {
//                        FileUtils.writeStringToFile(leafConfFile, "workerID=" + workerID, false);
//                        LOGGER.info("local file cache workerID is {}", workerID);
//                    }
//                } else {
//                    LOGGER.warn("create parent dir error===");
//                }
//            } catch (IOException e) {
//                LOGGER.warn("craete workerID conf file error", e);
//            }
//        }
//    }

    private CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    /**
     * 上报数据结构
     */
    static class Endpoint {
        private String ip;
        private String port;
        private long timestamp;

        public Endpoint() {
        }

        public Endpoint(String ip, String port, long timestamp) {
            this.ip = ip;
            this.port = port;
            this.timestamp = timestamp;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    public String getZk_AddressNode() {
        return zk_AddressNode;
    }

    public void setZk_AddressNode(String zk_AddressNode) {
        this.zk_AddressNode = zk_AddressNode;
    }

    public String getListenAddress() {
        return listenAddress;
    }

    public void setListenAddress(String listenAddress) {
        this.listenAddress = listenAddress;
    }

    public int getWorkerID() {
        return workerID;
    }

    public void setWorkerID(int workerID) {
        this.workerID = workerID;
    }

}
