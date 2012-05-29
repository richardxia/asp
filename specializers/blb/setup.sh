
#!/bin/bash

mkdir sejits
cd sejits
git clone git://github.com/richardxia/asp.git

cd ~/spark
sbt/sbt assembly

cd ~
mkdir asp_extra
cd asp_extra
wget http://pypi.python.org/packages/source/c/codepy/codepy-2012.1.2.tar.gz#md5=992482e56aa3f5351a08e4c39572bc3a
tar -zxvf codepy-2012.1.2.tar.gz
cd codepy-2012.1.2
python setup.py build
python setup.py install
yum install numpy
y

cd ~
mkdir avro
cd avro
wget http://www.fightrice.com/mirrors/apache//avro/avro-1.5.4/py/avro-1.5.4.tar.gz
tar -zxvf avro-1.5.4.tar.gz
cd avro-1.5.4
python setup.py build
python setup.py install

cd ..
mkdir java_avro
cd java_avro
wget http://www.fightrice.com/mirrors/apache//avro/avro-1.5.4/java/avro-1.5.4.jar
unzip avro-1.5.4.jar
mv org ~/avro

cd ..

wget http://jackson.codehaus.org/1.9.6/jackson-all-1.9.6.jar
unzip jackson-all-1.9.6.jar

export CLASSPATH=$CLASSPATH:~/avro:/root/spark/core/target/spark-core-assembly-0.4-SNAPSHOT.jar

cd ~/sejits/asp/specializers/blb
chmod +x test.sh




