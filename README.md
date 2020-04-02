# flink-service-discovery_plus


# 1 修改配置文件

```
  # 1. 修改flink配置
  flink-conf.yaml
  + metrics.reporters: prom
  + metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
  + metrics.reporter.prom.port: 9103
  
  # 2. 下载jar
     # 下载地址https://javalibs.com/artifact/org.apache.flink/flink-metrics-prometheus_2.11
    flink-metrics-prometheus_2.11-1.10.0.jar
    分发到集群中flink安装目录的lib下  
  

  #3 将py脚本里面的这行，修改为自己地址，主备自动跳转，不影响数据获取
  rm_addr = "http://yarn.xxx.com"
  
  # 4 RUN:
  python2 discovery.py
  
  #验证:
  time curl http://127.0.0.1:5000/flink
  [{"targets": ["p-cdh-dn-021.xxx.com:9103", "p-cdh-dn-042.xxx.com:9103"]}]
  
  
  # 5 Prometheus 添加配置
    
    #prometheus.yml
    - job_name: api_sd
      scrape_interval: 30s
      scrape_timeout: 30s
      scheme: http
      api_sd_configs:
      - api:
        - http://xx.xx.xx.xx:5000/flink
        refresh_interval: 1m
  

```

# 2 
  ## 2.1 Prometheus config
  
  <img src=https://github.com/wangbokun/flink-service-discovery_plus/blob/master/img/prometheus-config.jpg" width="50%">

