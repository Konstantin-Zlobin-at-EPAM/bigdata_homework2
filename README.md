# Homework 2, Big Data training, instruction:

## 0 - Preparation:

### build the application
>mvn clean package

### copy application jar to the following hdfs folder:
>hadoop fs -copyFromLocal -f homework2-1.0.jar hdfs:///apps/homework2/

### copy initial input file to hdfs:
> hdfs:///apps/homework2/user.profile.tags.us.txt

### Classes PagesDownloader and WordsPerPageCounter could be run locally
### just make sure your $HADOOP_HOME is set correctly
### also create /apps/homework2/ with write permissions for the current user 
### and put user.profile.tags.us.txt there  

## 1 - Downloading data set:
### After the first failed attempt, you need to open http://www.miniinthebox.com in a browser and enter the captcha, then run it again 
> yarn jar homework2-1.0.jar com.epam.bigdata.homework2.Client com.epam.bigdata.homework2.PagesDownloader 1 hdfs:///apps/homework2/homework2-1.0.jar

### In case you could not obtain data set from http://www.miniinthebox.com
> cd ./prepared_dataset

> hadoop fs -copyFromLocal -f intermediate hdfs:///apps/homework2/

## 2 - Word counting on a prepared data set 
> yarn jar homework2-1.0.jar com.epam.bigdata.homework2.Client com.epam.bigdata.homework2.WordsPerPageCounter 1 hdfs:///apps/homework2/homework2-1.0.jar

### it sould produce file hdfs:///apps/homework2/user.profile.tags.us.txt_out.txt

## That's it!
  