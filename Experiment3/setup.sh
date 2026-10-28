#!/usr/bin/env bash
# Run inside WSL Ubuntu, from the copied lab directory.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
for executable in curl tar python3 javac; do
  command -v "$executable" >/dev/null || { echo "Missing: $executable"; exit 1; }
done
test -x "$HADOOP_HOME/bin/hadoop" || { echo "Check HADOOP_HOME in env.sh"; exit 1; }
mkdir -p downloads software runtime logs results

download() {
  local url="$1" archive="$2"
  curl -fL --retry 3 --connect-timeout 30 -C - "$url" -o "downloads/$archive"
  curl -fL --retry 3 --connect-timeout 30 "$url.sha512" -o "downloads/$archive.sha512"
  python3 - "downloads/$archive" <<'PY'
import hashlib, pathlib, re, sys
p = pathlib.Path(sys.argv[1])
text = pathlib.Path(str(p) + '.sha512').read_text()
hashes = re.findall(r'\b[0-9a-fA-F]{128}\b', text)
if not hashes:
    # Apache checksum files can wrap the digest into groups.
    tail = text.split('=', 1)[-1] if '=' in text else text.split(':', 1)[-1]
    joined = ''.join(tail.split())
    if re.fullmatch(r'[0-9a-fA-F]{128}', joined):
        hashes = [joined]
with p.open('rb') as f:
    digest = hashlib.file_digest(f, 'sha512').hexdigest()
if digest not in [h.lower() for h in hashes]:
    raise SystemExit('SHA512 mismatch or unrecognized checksum: ' + str(p))
print('SHA512 OK:', p.name)
PY
}

if [[ ! -x "$HBASE_HOME/bin/hbase" ]]; then
  download https://archive.apache.org/dist/hbase/2.5.8/hbase-2.5.8-hadoop3-bin.tar.gz hbase-2.5.8-hadoop3-bin.tar.gz
  mkdir -p "$HBASE_HOME"
  tar -xzf downloads/hbase-2.5.8-hadoop3-bin.tar.gz -C "$HBASE_HOME" --strip-components=1
fi
if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
  download https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz spark-3.5.1-bin-hadoop3.tgz
  mkdir -p "$SPARK_HOME"
  tar -xzf downloads/spark-3.5.1-bin-hadoop3.tgz -C "$SPARK_HOME" --strip-components=1
fi

# Only this lab's HBase is configured. The existing HDFS data is untouched.
python3 - <<'PY'
import os, pathlib, shutil, time, xml.etree.ElementTree as ET
root = pathlib.Path(os.environ['LAB3_ROOT'])
hbase = pathlib.Path(os.environ['HBASE_HOME'])
conf = ET.Element('configuration')
for name, value in {
    'hbase.rootdir': (root / 'runtime/hbase-data').as_uri(),
    'hbase.cluster.distributed': 'false',
    'hbase.zookeeper.property.dataDir': str(root / 'runtime/zookeeper'),
    'hbase.tmp.dir': str(root / 'runtime/hbase-tmp'),
    'hbase.unsafe.stream.capability.enforce': 'false',
    'hbase.zookeeper.quorum': 'localhost',
}.items():
    p = ET.SubElement(conf, 'property')
    ET.SubElement(p, 'name').text = name
    ET.SubElement(p, 'value').text = value
ET.indent(conf)
ET.ElementTree(conf).write(hbase / 'conf/hbase-site.xml', encoding='UTF-8', xml_declaration=True)
with (hbase / 'conf/hbase-env.sh').open('a') as f:
    f.write('\nexport JAVA_HOME=' + os.environ['JAVA_HOME'] + '\n')
    f.write('export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true\n')
    f.write('export HBASE_HEAPSIZE=1024\n')

# Preserve the user's Hadoop properties and back up before the one missing setting.
path = pathlib.Path(os.environ['HADOOP_CONF_DIR']) / 'mapred-site.xml'
parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
tree = ET.parse(path, parser=parser)
config = tree.getroot()
name = 'mapreduce.application.classpath'
existing = next((p for p in config.findall('property') if p.findtext('name') == name), None)
if existing is None:
    backup = pathlib.Path(str(path) + '.lab3-backup-' + time.strftime('%Y%m%d-%H%M%S'))
    shutil.copy2(path, backup)
    p = ET.SubElement(config, 'property')
    ET.SubElement(p, 'name').text = name
    hadoop = os.environ['HADOOP_HOME']
    ET.SubElement(p, 'value').text = hadoop + '/share/hadoop/mapreduce/*:' + hadoop + '/share/hadoop/mapreduce/lib/*'
    ET.indent(tree)
    tree.write(path, encoding='UTF-8', xml_declaration=True)
    print('Added MapReduce application classpath; backup:', backup)
else:
    print('Existing MapReduce application classpath preserved:', existing.findtext('value'))
PY
echo 'Setup finished. Run: source ./env.sh'
echo 'Then: bash start-hadoop.sh'
