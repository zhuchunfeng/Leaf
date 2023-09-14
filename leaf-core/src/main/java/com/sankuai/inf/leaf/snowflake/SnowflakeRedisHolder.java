package com.sankuai.inf.leaf.snowflake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sankuai.inf.leaf.common.*;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SnowflakeRedisHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRedisHolder.class);

    //最大能够分配的workerid =1023
    private int workerID;

    //服务ip  服务端口
    private String ip;
    private int port;

    //redis 配置
    private String redisIp;
    private Integer redisPort;
    private String redisPassword;
    private Jedis jedis;

    private long lastUpdateTime;

    public SnowflakeRedisHolder(String ip, int port, String redisIp,Integer redisPort,String redisPassword) {
        this.ip = ip;
        this.port = port;

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

            //获取当前集群所有节点的workerId
            Set<String> nodesWorkIds = getNodesWorkerId(redisKeyNodes);

            //生成当前节点的workerId
            int currentWorkerId;

            do {
                //获取workerId
                Long incr = jedis.incr(redisKeyCurrent);
                currentWorkerId = (int)(incr % 1024);

                //避免极端情况下的workerId重复
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

    /**
     *  获取当前集群所有节点的workerId
     * @param redisKeyNodes 集群节点workerId上报的redis路径
     * @return 所有节点的workerId
     */
    private Set<String> getNodesWorkerId(String redisKeyNodes) {
        Set<String> nodesWorkIds=new HashSet<>();

        final int keyLength = redisKeyNodes.split(":").length;

        Set<String> keys = jedis.keys(redisKeyNodes+":*");
        for (String key : keys) {
            String[] split = key.split(":");
            String nodeWorkId = split[keyLength];
            nodesWorkIds.add(nodeWorkId);
        }
        return nodesWorkIds;
    }


    private void ScheduledUploadData(final Jedis jedis, final String redisKeyNodes) {
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
                updateNewData(jedis,workerID,redisKeyNodes);
            }
        }, 1L, 3L, TimeUnit.SECONDS);//每3s上报数据

    }

    private void updateNewData(Jedis jedis, int workerID, String redisKeyNodes) {
        try {
            if (System.currentTimeMillis() < lastUpdateTime) {
                return;
            }
            String key = redisKeyNodes+":"+workerID;
            jedis.set(key,buildData());
            jedis.expire(key,30L);  //redis续期时间30s
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



    public int getWorkerID() {
        return workerID;
    }

    public void setWorkerID(int workerID) {
        this.workerID = workerID;
    }

}
