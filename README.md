# StackOverflowAnalysis
=========================================

In this project we worked on the StackOverflow dataset. Stackoverflow is one of the largest websites for technical discussion and we wanted to use this data to perform analysis which may be helpful to the users of this website in some way. 

- **Analysis Task1**: 
In this analysis we find the top contributors in each category(tags). Once we have the top contributors we can suggest users to tag those top contributors in the questions. We think that this information may help users to get answers to questions quickly by tagging such contributors in the question. Of course the tagging functionality has to be implemented by the website to support this and is not in our scope.

- **Analysis Task2**:
Find the response time to questions for each category(tags). Our hypothesis is “higher the frequency of category lesser is the response time” where response time is measured as the time difference between the question posted and the first answer posted. We will determine if the data supports the hypothesis or not. This analysis can be used to understand what are the popular tags and get an insight if there is any relation between the popularity and response time .

Apart from that we will analyze the performance of theta join using HBase and Hive. For that we will do the analysis 1 using HBase and Hive then compare their performance. We will explore various ways of using HBase as index to compare their respective performances.

