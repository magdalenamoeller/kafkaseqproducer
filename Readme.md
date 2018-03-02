#### Local setup

* git clone git@github.com:magdalenamoeller/kafkaseqproducer.git
* cd kafkaseqproducer
* build application: 
    * mvn clean install
* run application:
    * java -cp target/kafkaseqproducer-1.0-SNAPSHOT.jar com.zanox.SequenceProducer test 172.17.0.1:9092