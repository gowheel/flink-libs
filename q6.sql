-- -- 开启 mini-batch
-- SET table.exec.mini-batch.enabled=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
-- SET table.exec.mini-batch.allow-latency=1s;
-- -- 一个 mini-batch 中允许最多缓存的数据
-- SET table.exec.mini-batch.size=1000;
-- -- 开启 local-global 优化
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
--
-- -- 开启 distinct agg 切分
-- SET table.optimizer.distinct-agg.split.enabled=true;


-- source
CREATE TABLE user_log (
    user_id VARCHAR,
    item_id VARCHAR,
    message VARCHAR
) WITH (
    'connector' = 'kafka',
    'scan.startup.mode' = 'earliest-offset',
    'topic' = 'user_behavior3',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'json'
);

-- sink
CREATE TABLE behavior_sink (
    rownum BIGINT,
    category_id VARCHAR,
    pv BIGINT,
    PRIMARY KEY (rownum) NOT ENFORCED
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://localhost:3306/flink_test',
    'connector.table' = 'pv_sink',
    'connector.username' = 'root',
    'connector.password' = 'admin123',
    'connector.write.flush.max-rows' = '1'
);


CREATE VIEW view_behavior AS
SELECT
    user_id,
    item_id,
    STR_TO_MAP(message, ';', '=')['category_id'] as category_id,
    STR_TO_MAP(message, ';', '=')['behavior'] as behavior
FROM user_log;

INSERT INTO behavior_sink
SELECT rownum, category_id, pv
FROM (
  SELECT *,
     ROW_NUMBER() OVER (ORDER BY pv desc) AS rownum
  FROM (
        SELECT
            category_id,
            count(1) AS pv
        FROM view_behavior
        GROUP BY category_id
    ) a
) t
WHERE rownum <= 10;
