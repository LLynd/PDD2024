. ./settings.sh

echo "start of 1download.sh"

mkdir $home/download
mkdir $home/cluster

cd $home/download
export PATH=/home/hadoop/.local/bin:$PATH
pip install --upgrade pip
pip install --upgrade --no-cache-dir gdown
gdown "https://drive.google.com/u/1/uc?id=1mYED2niZ7hxFRdAUIf9KDk0HIE2esCR1&confirm=t"
wget -P $home/download https://dlcdn.apache.org/hadoop/common/$hadoop_name/$hadoop_name_gz
wget -P $home/download https://dlcdn.apache.org/spark/$spark_url_dir/$spark_name_gz

tar -xvf $home/download/jdk-8u181-linux-x64.tar.gz -C $home/cluster
tar -xvf $home/download/$hadoop_name_gz -C $home/cluster
tar -xvf $home/download/$spark_name_gz -C $home/cluster
rm $home/download/jdk-8u181-linux-x64.tar.gz
rm $home/download/$hadoop_name_gz
rm $home/download/$spark_name_gz

echo "end of 1download.sh"
