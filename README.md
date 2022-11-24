# Tweets-Sentiment-Analysis
<p align="center">
      <img src="https://github.com/dadoPuccio/Tweets-Sentiment-Analysis/blob/main/images/logo.png" width="400px"/>
</p>

__Sentiment Classifier of Tweets, based on Lambda Architecture.__  
This application makes use of Lambda Architecture to perform real-time sentiment analysis on Tweets in a big data scenario.

## Requirements
In order to run this application is needed a correct installation and configuration of the following technologies:

- [JDK](https://www.oracle.com/java/technologies/downloads/) - _11 or above_
- [Apache Storm](https://storm.apache.org/) - _2.4.0_
- [Apache Hadoop](https://hadoop.apache.org/) - _3.2.4_
- [Apache HBase](https://hbase.apache.org/) - _2.4.15_

Other dependencies such as [LingPipe](http://www.alias-i.com/lingpipe/) and [JavaFX](https://openjfx.io/) are automatically added through [Maven](https://maven.apache.org/).

## Usage
Before running the application it is required to add your ```BEARER_TOKEN``` to the configuration file ```src/main/resources/gui/credentials.json``` in order to access [Twitter API v2](https://developer.twitter.com/en/docs/twitter-api). In case you don't have one you can request it [here](https://developer.twitter.com/en/portal).

To run the application make sure that Apache Storm, Apache Hadoop and Apache HBase are currently running in your configuration, then launch ```src/main/java/gui/GUIStarter main()```  after compiling the project through Maven. 

The usage of an IDE such as [IntelliJ Idea](https://www.jetbrains.com/idea/) is highly recommended.

### Welcome Page
This is the first page displayed when launching the application. Here you can enter the keywords you wish to analyze. When clicking ```Start Analysis``` the architecture is started.

<p align="center">
      <img src="https://github.com/dadoPuccio/Tweets-Sentiment-Analysis/blob/main/images/welcome.png" width="400px"/>
    </p>
    <br/>

### Main Page
This is the main page of the application where the real-time results of the analysis are reported. On the top-right corner the dedicated button allows to stop the architecture.

<p align="center">
      <img src="https://github.com/dadoPuccio/Tweets-Sentiment-Analysis/blob/main/images/mainpage.png" width="600px"/>
    </p>
    <br/>

### Example
Here is reported a demo video of the application:

https://user-images.githubusercontent.com/40762525/203812367-2897a7db-08d6-4048-823e-5b029faef8d7.mp4

## Acknowledgements
This work is a full term project for the course of Parallel Computing, held by professor Marco Bertini at University of Florence.  
Further information over this implementation is available in the [report](https://github.com/dadoPuccio/Tweets-Sentiment-Analysis/blob/main/Full_Term_Report.pdf).
