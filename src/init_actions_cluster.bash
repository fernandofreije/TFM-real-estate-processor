
#!/bin/bash

# Install tools
apt-get -y install python3 python-dev build-essential python3-pip
easy_install3 -U pip

# Setup python3 for Dataproc
echo "export PYSPARK_PYTHON=python3" | tee -a  /etc/profile.d/spark_config.sh  /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "export PYTHONHASHSEED=0" | tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /usr/lib/spark/conf/spark-env.sh
echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
echo 'export PYTHONIOENCODING=utf8'
