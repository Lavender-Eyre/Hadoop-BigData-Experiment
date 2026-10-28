# 实验三：HBase 操作与 PageRank 算法实现

本目录包含 HBase Shell 与 Java API 实验，以及 Hadoop MapReduce、Spark 两种 PageRank 实现。两种 PageRank 程序均连续迭代 10 次，输出 `(网页,PR值)`，保留 10 位小数。

## 环境

- WSL 2 / Ubuntu，Java 8
- 已有 Hadoop 3.3.6，HDFS 与 YARN 伪分布式运行
- HBase 2.5.8-hadoop3，独立单机模式
- Spark 3.5.1-bin-hadoop3，使用 Java RDD API，提交到 YARN
- Python 3：仅使用标准库，实现 Hadoop Streaming 和结果检查

`env.sh` 默认沿用已有 `JAVA_HOME`、`HADOOP_HOME`；未设置时分别使用 `/usr/lib/jvm/java-8-openjdk-amd64` 和 `$HOME/hadoop`。运行前应确认它们指向正确的安装目录。

## 文件说明

| 路径 | 用途 |
| --- | --- |
| `hbase/HBaseLab.java` | 创建表、插入、扫描、修改、删除、列族操作、清空与行数统计 |
| `hbase/shell-demo.hbase` | 对应的 HBase Shell 命令和完整样例数据 |
| `hbase/run-java.sh` | 编译、运行 HBase Java 实验 |
| `pagerank/data.tsv` | 自定义有向图数据，字段间使用真正的 TAB |
| `pagerank/normalize.py` | 合并同源记录、去重出链、补齐仅出现在目标中的网页 |
| `pagerank/mr.py` | Streaming Mapper / Reducer |
| `pagerank/run_mr.sh` | 提交 10 轮 PageRank 的 MapReduce 作业 |
| `pagerank/SparkPageRank.java` | Spark RDD PageRank |
| `pagerank/graph.py` | 输入读取与串行参考算法 |
| `pagerank/compare.py` | 比较两版结果、参考算法、输出精度与总分 |
| `setup.sh` | 下载校验软件、配置 HBase，并备份后补充缺少的 MapReduce 类路径 |
| `start-hadoop.sh` | 启动尚未运行的 Hadoop 服务并打印状态 |

## 运行步骤

下面命令均在 Ubuntu Bash 中执行；Windows PowerShell 用户先执行 `wsl -d Ubuntu`。进入本仓库的 `Experiment3` 目录。

### 1. 配置环境与启动服务

```bash
source ./env.sh
set -o pipefail
bash setup.sh
source ./env.sh
hbase version
spark-submit --version
bash start-hadoop.sh
```

必须确认 `hdfs dfsadmin -report` 中有存活 DataNode，`yarn node -list` 中有 RUNNING 节点，且 `hdfs dfsadmin -safemode get` 为 OFF。已有 HDFS 不要再次格式化。如处于安全模式，先检查数据块与日志，不要盲目退出安全模式。

HBase 的数据保存在本目录的 `runtime/`，使用单机模式避免混用其内置 Hadoop 库与现有集群库。PageRank 的数据和计算使用 HDFS/YARN。

### 2. HBase 实验

```bash
"$HBASE_HOME/bin/start-hbase.sh"
printf "status 'simple'\nexit\n" | hbase shell -n
hbase shell -n hbase/shell-demo.hbase 2>&1 | tee logs/hbase-shell.log
bash hbase/run-java.sh
```

脚本使用独立的 `lab3_*` 表，重复执行会按照题目要求重建这些实验表。学生表、课程表各 3 行；选课表按学生聚合为 3 行，包含 6 个成绩单元格。修改、删列族、删除行、清空操作在单独的演示表中完成。

测试交互式修改成绩，提示后输入新值：

```bash
java -cp "build/hbase-lab.jar:$(hbase classpath)" HBaseLab modify lab3_sc_api Zhangsan Score:Math
```

题目中的 Mary 学号与 Lisi 重复，本实验根据选课表按 Mary=2015002 处理。题目三参数 `modifyData` 未提供新值参数，因此保留该签名并从终端读取新值，同时提供四参数重载。`scanColumn` 会遍历所有现存行，对缺失单元格输出 `null`。

### 3. 准备输入

```bash
python3 pagerank/normalize.py pagerank/data.tsv prepared
mkdir -p results logs
RUN="/user/$(whoami)/lab3-$(date +%Y%m%d-%H%M%S)"
printf '%s\n' "$RUN" > results/hdfs-run.txt
hdfs dfs -mkdir -p "$RUN/input"
hdfs dfs -put prepared/initial.tsv prepared/normalized.tsv "$RUN/input/"
N=$(cat prepared/node_count.txt)
```

`prepared` 仅需创建一次，程序拒绝覆盖已有目录。每次完整重跑应使用新的 `RUN`，避免覆盖旧结果。关闭终端后，可重新加载 `env.sh`，然后用 `RUN=$(cat results/hdfs-run.txt)` 和 `N=$(cat prepared/node_count.txt)` 恢复变量。

### 4. MapReduce

```bash
bash pagerank/run_mr.sh "$RUN/input/initial.tsv" "$RUN/mr" "$N" 2>&1 | tee logs/mr-run.log
hdfs dfs -cat "$RUN/mr/iter10/part-*" > results/mr-result.txt
```

每轮先通过一个作业求悬挂节点总分，再通过一个作业更新排名。因此 10 轮共 20 个 MapReduce 作业。Driver 仅读取悬挂节点总分这一标量；排名更新在 Mapper/Reducer 中完成。最终一轮使用 `stream.reduce.output=keyonlytext`，避免默认文本输出附加行尾 TAB。

### 5. Spark

```bash
mkdir -p build/spark
javac -encoding UTF-8 -source 8 -target 8 -cp "$SPARK_HOME/jars/*" -d build/spark pagerank/SparkPageRank.java
jar cf build/spark-pagerank.jar -C build/spark .
spark-submit --master yarn --deploy-mode client \
  --class SparkPageRank \
  --num-executors 1 --executor-cores 2 \
  --executor-memory 1g --driver-memory 1g \
  build/spark-pagerank.jar "$RUN/input/normalized.tsv" "$RUN/spark" \
  2>&1 | tee logs/spark-run.log
hdfs dfs -cat "$RUN/spark/part-*" > results/spark-result.txt
```

使用 Spark on YARN，无须另启 Spark Master/Worker。

### 6. 验证

```bash
python3 pagerank/compare.py pagerank/data.tsv results/mr-result.txt results/spark-result.txt | tee results/comparison.txt
```

比较按网页名进行，不依赖输出行序；同时检查节点集合、10 位小数、串行参考结果和总 PR。

## 算法与样例结果

初始每个网页 PR=1，阻尼系数 d=0.85。每轮使用：

```text
PR_next(v) = 0.15 + 0.85 * (sum(PR(u)/outdegree(u), u -> v) + dangling_sum/N)
```

无出链网页的总分均分给全部 N 个网页，故总 PR 约等于 N。固定迭代 10 次并不表示已证明完全收敛。

样例：A→B,C；B→C；C→A；D→C；E 无出链。10 轮参考结果：

```text
(A,1.8039149851)
(B,0.9399665479)
(C,1.8946726508)
(D,0.1807229081)
(E,0.1807229081)
```

## 关闭服务

确认没有其他作业使用集群后：

```bash
"$HBASE_HOME/bin/stop-hbase.sh"
yarn --daemon stop nodemanager
yarn --daemon stop resourcemanager
hdfs --daemon stop secondarynamenode
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

安装包、软件目录、运行数据、日志和编译产物均由 `.gitignore` 排除。
