# SocialCircle
An end-to-end project to build an imaginary social media platform - SocialCircle. This is to demonstrate events processing in (near) real time using open source platforms like Spark, Kafka, Redis, Elasticsearch, Neo4J and Grafana. 

The eventual goal of this project is to simulate real world events like 
* user onboarding on the platform 
* user following/unfollowing other users

The SocialCircle would be eventually visualized on a Neo4J powered real-time dashboard to show how different users are getting connected/disconnected on real time. 
Some analytical dashboards can also be built on Grafana/Qlik to show how each user is performing on the platform. 

These events would be further analyzed in a time-series dashboard built on Grafana (or similar tool) for realtime dashboards to capture below KPIs:
* Ranking
* Trends
* Follower ratio (number of followers divided by number of users the user follows) 
 
