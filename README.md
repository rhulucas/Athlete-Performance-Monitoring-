Athlete Performance Monitoring (PySpark + MongoDB)

## Overview
Analyzed **1Hz biometric time-series data** (heart rate, activity) from athletes to monitor performance and fatigue.  
Implemented rolling averages and peak detection using **PySpark**, stored data in **MongoDB**, and visualized results with **Matplotlib**.

## Data
- Stored in MongoDB (Atlas or local).  
- Fields: `activity`, `heart_rate`, `metadata.session`, `metadata.athleteId`, `timestamp`.  
  Requires your own MongoDB credentials.

## Requirements
- Python 3.8+, Spark/PySpark  
- Packages: `pandas`, `pyspark`, `scipy`, `matplotlib`, `pymongo`

## Run
1. Update MongoDB URI in code.  
2. Run:  
   ```bash
   python main.py

<img width="2000" height="1200" alt="activity_peaks" src="https://github.com/user-attachments/assets/33a7612c-bde1-4fda-89ba-820dd7ef3489" />
<img width="2000" height="1200" alt="activity_rolling_average" src="https://github.com/user-attachments/assets/73b26923-d272-45d4-a839-14ac46f8b29c" />
<img width="2000" height="1200" alt="rolling_average_only" src="https://github.com/user-attachments/assets/0c0cc7a3-0b59-4c3d-9a04-c02f02b38189" />
