Project: STEDI Human Balance Analytics
---
The STEDI team has the product for the step trainer.
Several customers have already received thier Step trainers, installed the mobile application, and begun using the, together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions. 

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. **Privacy will be a primary consideration in deciding what data can be used.**

We are requested to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

There are 3 tables in this project that we need to build the lake house solution

1. Customers table: Can join with the table accelerometer by using email and join with step trainer table by serialnumber. 

| Column        | Key   |
| ------------- | :-----:|
| serialnumber      | PK |
|email|FK|
| sharewithpublicasofdate||
| birthday |  |
|registrationdate||
|sharewithresearchasofdate||
|customername||
|lastupdatedate||
|phone|| 
|sharewithfriendasofdate||

2. Accelerometer Records table: Can join with the table customer by using email and join with step trainer table by timeStamp and sensorReadingTime. 

| Column        | Key   |
| ------------- | :-----:|
|timeStamp  | FK |
|user(email)|FK|
| x||
| y ||
|z||

3. Step Trainer Records table: Can join with the table customer by serialnumber and join with accelerometer table by timeStamp and sensorReadingTime. 

| Column        | Key   |
| ------------- | :-----:|
| sensorReadingtime|FK|
| serialNumber      | FK |
| distanceFromObject||

From the requirement from the data scientist team, we need to create the data for machine learning from the data. From the data, we build 3 zone of the tables such as `landing`, `trusted`, and `curated` zone.

1. `landing zone`: Raw data of 3 table
2. `trusted zone`
    
    2.1. `customer_trusted` table: Filter only customer who accept to share the data for the research. (sharewithresearchasofdate not equal to zero)
    
    2.2. `accelerometer_trusted` table: Filter only the data of the customer who accept to share the data. (`accelerometer_landing` inner join with `customer_trusted`)

    2.3 `step_trainer_trusted` table: Filter only the data of the customer who accept to share the data. (`step_trainer_landing` inner join with `customer_curated`)

3. `curated zone`

    3.1. `customer_curated` table: Filter customer who have the accelerometer data (`customer_trusted` inner join with `accelerometer_trusted`)

    3.2. `machine_learning_curated` table: the result from inner join between table `step_trainer_trusted` and `accelerometer_trusted`

---
Project Execution
---
1. Manually build the 3 landing tables in AWS Athena consist of `customer_landing`, `accelerometer_landing`, and `step_trainer_landing`.
2. Create the glue job for `customer_trusted`, `accelerometer_trusted`, and `customer_curated` repectively.
3. Create the glue job for `step_trainer_trusted`
4. Create the glue job for `machine_learning_curated`