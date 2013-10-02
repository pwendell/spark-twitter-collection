How to run this from scratch on EC2:

1. Launch Ubuntu 12.04 on Amazon (m1.medium)
2. sudo apt-get update
3. sudo apt-get install -y openjdk-7-jdk
4. sudo apt-get install -y git
5. git clone https://github.com/pwendell/spark-twitter-collection.git
export AWS_ACCESS_KEY_ID=XXX
export AWS_SECRET_ACCESS_KEY=YYY
export OUTPUT_BATCH_INTERVAL=3600
export OUTPUT_DIR=ZZZ
6. cp credentials.txt.template credentials.txt
7. Fill in Twitter credentials
8. sbt/sbt run  
