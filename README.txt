MapReduce PageRank
------------------

PageRank, as many other graph algorithms does not fit the MapReduce programming
model. It's just a different kind of nail for a very nice hammer.

However, if you really want, you can bang here and there and get it done.

  mvn hadoop:pack (see: http://github.com/akkumar/maven-hadoop)
  
  hadoop fs -mkdir hdfs://localhost/user/castagna/src/
  hadoop fs -mkdir hdfs://localhost/user/castagna/src/test
  hadoop fs -mkdir hdfs://localhost/user/castagna/src/test/resources

  hadoop fs -copyFromLocal src/test/resources/* hdfs://localhost/user/castagna/src/test/resources/
  
  hadoop fs -rmr hdfs://localhost/user/castagna/target/

  mvn hadoop:pack; hadoop --config conf/hadoop-local.xml jar ./target/hadoop-deploy/pagerank-hdeploy.jar com.talis.labs.pagerank.mapreduce.PageRank ./src/test/resources/datasets/small/ ./target/output 30 0.00001


Maven
-----

Once you have installed Maven, you can have fun with the following commands:

  mvn -Declipse.workspace=/opt/workspace eclipse:add-maven-repo
  mvn eclipse:clean eclipse:eclipse -DdownloadSources=true -DdownloadJavadocs=true
  mvn dependency:resolve
  mvn compile
  mvn test
  mvn package
  mvn site
  mvn install
  mvn deploy




                                          -- Paolo Castagna, Talis Systems Ltd.
