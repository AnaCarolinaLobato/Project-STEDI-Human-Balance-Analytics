# STEDI Human Balance Analytics Project

## Introduction

In this project, the data engineering team at STEDI aims to build a robust data lakehouse solution for sensor data to train a machine learning model.

## Project Details

The STEDI Team has developed a hardware STEDI Step Trainer with the following features:

- Trains users in STEDI balance exercises.
- Equipped with sensors to collect data for training a machine-learning algorithm to detect steps.
- Comes with a companion mobile app that collects customer data and interacts with device sensors.

Millions of early adopters have expressed interest in purchasing and using the STEDI Step Trainers. Some customers have already received their Step Trainers, installed the mobile application, and started testing their balance. The Step Trainer is essentially a motion sensor that records the distance of the object detected, while the app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

To train a machine learning model accurately in real-time, the STEDI team aims to use motion sensor data. Privacy is a primary consideration, and only data from customers who have agreed to share their information for research purposes will be used.

## Implementation

### Glue Jobs

The Python code for Glue jobs has been implemented in Python files located in the `src` folder, which is generated automatically from Glue Studio.

### Athena Queries

Results from Athena queries on the `customer_landing` and `accelerometer_landing` tables have been captured and saved in the `screenshots` folder. CSV result files in the `screenshots` folder visualize the results due to limited screen resolution.

### Scripts Files

The `scripts` files represents the auto-generated SQL of the Glue tables from Athena.

## Glue Tables

```puml
entity "customer_landing" as customer_landing {
  customerName: string,
  email: string,
  phone: string,
  birthDay: string,
  serialNumber: string,
  registrationDate: bigint,
  lastUpdateDate: bigint,
  shareWithResearchAsOfDate: bigint,
  shareWithPublicAsOfDate: bigint
}

entity "customer_trusted" as customer_trusted {
  customerName: string,
  email: string,
  phone: string,
  birthDay: string,
  serialNumber: string,
  registrationDate: bigint,
  lastUpdateDate: bigint,
  shareWithResearchAsOfDate: bigint,
  shareWithPublicAsOfDate: bigint
}

entity "customer_curated" as customer_curated {
  customerName: string,
  email: string,
  phone: string,
  birthDay: string,
  serialNumber: string,
  registrationDate: bigint,
  lastUpdateDate: bigint,
  shareWithResearchAsOfDate: bigint,
  shareWithPublicAsOfDate: bigint
}

entity "accelerometer_landing" as accelerometer_landing {
   user: string,
   timeStamp: bigint,
   x: float,
   y: float,
   z: float
}

entity "accelerometer_trusted" as accelerometer_trusted {
   user: string,
   timeStamp: bigint,
   x: float,
   y: float,
   z: float
}

entity "step_trainer_trusted" as step_trainer_trusted {
  sensorReadingTime: bigint,
  serialNumber: string,
  distanceFromObject: bigint
}

entity "machine_learning_curated" as machine_learning_curated {
  timestamp: bigint,
  x: float,
  y: float,
  z: float,
  sensorreadingtime: bigint,
  serialnumber: string,
  distancefromobject: bigint
}
