# 📈 Real-time Stock Price Prediction and Visualization

## 🧠 Project Overview

This project builds a real-time stock analysis and visualization system using modern data streaming and processing technologies like Apache Kafka, Apache Spark, Docker, and TimescaleDB.

> 🎯 Goal: Collect real-time stock data, predict stock prices using an LSTM model (TensorFlow), and visualize the results through interactive dashboards.

---

## 🏗️ System Architecture


### Components:

1. **Data Source**
   - Real-time data from IoT devices, SCADA, CCTV, stock indexes, weather APIs, etc.
   - The data is cleaned and converted into a structured format.

2. **Kafka Producer**
   - Streams structured data into Apache Kafka.

3. **Apache Spark**
   - Consumes data from Kafka.
   - Performs real-time processing: trend analysis, anomaly detection, and stock price prediction using LSTM.
   - Writes the processed data into a PostgreSQL database.

4. **PostgreSQL + TimescaleDB**
   - Efficient storage for time-series stock data.

5. **Grafana**
   - Provides a real-time dashboard to visualize market trends and model predictions.

6. **Jupyter Notebook**
   - Used for LSTM model development and experimentation.

7. **Docker**
   - Containerizes all components for easy deployment and scalability.

---

## 🚀 Key Features

- ✅ Real-time data collection and processing
- 📊 Interactive visualizations using Grafana
- 🔍 Anomaly and trend detection
- 📦 Scalable, containerized architecture with Docker
- 🧠 Stock price prediction using LSTM in TensorFlow
- 📁 Jupyter Notebooks for model training and analysis

---

## 🧪 Tech Stack

| Technology       | Purpose                            |
|------------------|-------------------------------------|
| Apache Kafka     | Real-time data streaming            |
| Apache Spark     | Stream processing                   |
| TensorFlow       | LSTM-based stock prediction         |
| TimescaleDB      | Time-series database (on Postgres)  |
| PostgreSQL       | Relational database backend         |
| Grafana          | Dashboards and data visualization   |
| Jupyter Notebook | ML development and testing          |
| Docker           | Containerization and orchestration  |

## Project Flow Chart
![flowchart](https://user-images.githubusercontent.com/90943529/217798777-82aae959-6260-4d0e-80a6-d8c674b77225.png)






