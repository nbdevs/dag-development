# Airflow ETL Pipeline

![image](https://airflow.apache.org/images/feature-image.png "Airflow is the automation orchestration tool.")

Child repository which stores all of the ETL development used to aid the pipeline. Contains ETL pipeline which is orchestrated by airflow to deliver data from source API to database to data warehouse and finally to PowerBI for visualizations.

## Summary of CICD Workflow

Top level data flow diagram representing the overall  architecture of the DAG ETL system.
<div id="CI/CD Pipeline for local development" align="center">
    <img width="755" alt="Screenshot 2023-08-02 at 12 15 20" src="https://github.com/nbdevs/dag-development/assets/75015699/3b1a8a0c-8e8a-4c0f-9682-093015187987"/>
</div>

A trigger event (which could be a git push, git pull, merge request of development code) activates the CI/CD pipeline via a git webhook at the repository site triggers the Jenkins multi-branch pipeline build. Jenkins is the automation tool that was used to automate CI/CD, and submodules were implemented to section off DAG code from the Dockerfile image to ensure that every change is tagged and versioned for the purpose of debugging.

This process takes place within a docker container which is necessary for spinning up instances as and when required – a feature of development and testing – where the changes are screened via build and deployment tests in the Jenkinsfile which utilises the docker-cli.

As shown in the diagram, if unsuccessful at any point of testing (build/deployment) then an updated commit status is sent to GitHub which indicates an error and alerts the data engineer. On the other hand, successful changes are then merged into the main branch to progress onto the goal of merging to the airflow master repository.

### Requirements Related to ETL System

| **Requirements**| **Description**| **Rationale**|
| -------- | -------- | -----|
|Inter-Class Interaction |All classes in the system shall implement from an abstract interface. | To reduce the coupling of classes to one another in the system.|
|Structure of System | The system shall be split into data source, data collection, data processing, data storage and data presentation layers. | Seperation of concerns. |
|Choice of Orchestration tool | The system shall use Apache Airflow as an orchestration tool for ETL.| To utilize the functionality of DAGs for jobs monitoring and completion.|
|Cloud Provider|Amazon Web Services (AWS) shall be the cloud provider of the system.|It will provide the data lake S3 for file storage, and EC2 nodes.|
|Choice of Data Warehouse|The system shall use Snowflake as the data warehouse.|It is the only data warehouse built entirely for the cloud which has means inbuilt horizontal scalability, whilst also being ACID compliant in terms of SQL transactions within the database.|
|Structure of Airflow repository|All DAGs shall have their own repositories and be submodules of the airflow repository|This is so that version control and monitoring can be in place for each DAG and to aid debugging, or in case a revision of a prior “correct” state of a DAG needs to be reapplied.|
|Data Cache – S3|Eliminate computational expensive database queries (reads and writes) by caching the data into AWS S3.|This will enable high availability of data and quick look ups.|

### Class Design

The objectives are to extract and load the data taken from the fastf1 API into the database environment ready for transformations. For this stage a Dockerfile will be used to provide deeper customisation, utilising the previously created PostgreSQL image created by the database administrator and including within it the base airflow image which now must be iterated upon in a similar manner with python application code for DAG development.

<div id="Overall class system for ETL development" align="center">
    <img width="850" alt="Screenshot 2023-08-02 at 12 17 31" src="https://github.com/nbdevs/dag-development/assets/75015699/18c90d2d-af11-44a3-805e-f6941e6051d9"/>
</div>

The connection subsystem is pictured below, and was designed to manage the connections to the database from clients, as well as to have a centralized place to store connection information so that connection pooling can be implemented to reuse existing objects, rather than creating new expensive connections at every request.

![connection](https://github.com/nbdevs/dag-development/assets/75015699/7195fac1-0f83-4de6-bdb9-06f1d766b397 "Connection Subsystem")

Since there is a requirement now for orchestration of the build process, the director class is used to manage the two ETL processes and calls the sequences of functions in the necessary order required to product completion, which consequentially results in the transformation of the initial DataFrame which stores the data, into the transformed table loaded into the warehouse. A reference to the Processor class is contained within the class as a compositional relationship which enables the utilization of either the DatbaseETL or WarehouseETL class to be called as a benefit of polymorphism.

![etl subsystem](https://github.com/nbdevs/dag-development/assets/75015699/6a5b9b9c-6a4a-4670-ba16-3198d1f56199 "ETL Subsystem")

---

## Environment Set Up (MacOS)

```bash
mkdir project_folder # create new folder to store virtual environment within
```

```bash
python3 -m venv project_env/project_folder  # create new virtual environment within folder
```

```bash
source dag_env/project_folder/bin/activate # activate the virtual environment
```

## Docker Container Management

```Bash
docker-compose down --volumes # stop existing docker containers spinning
```

```bash
docker rmi -f $(docker images -aq) # delete docker images which are taking up space
```

```bash
docker volume rm $(docker volume ls -q) # delete docker volumes which are taking up space
```

```bash
docker system prune -a # prune system of dangling cache
```

```bash
docker builder prune -a # prune builder of dangling cache
```

```bash
docker compose up -d # spin up container in detached form
```

### Languages and Tools :hammer_and_wrench:

<div>
  <img src="https://user-images.githubusercontent.com/75015699/158418219-28257172-616b-484e-b0bf-e6e9ce9c59a1.png" title="Python" alt="Python" width="40" height="40"/>&nbsp;
  <img src="https://www.vectorlogo.zone/logos/snowflake/snowflake-ar21.png" title="Snowflake" alt="Snowflake" width="80" height="40"/>&nbsp;
  <img src="https://developers.redhat.com/sites/default/files/styles/article_feature/public/blog/2014/05/homepage-docker-logo.png?itok=zx0e-vcP" title="Docker" alt="Docker" width="50" height="40"/>&nbsp;
</div>

## Status

Ongoing development, major changes planned which will provide extra functionality.

Pending:

- [ ] ETL functionality for Telemetry Grain
- [ ] ETL Functionality into Data Warehouse.
- [ ] Data visualisation dashboards for Telemetry Grain.

## License

Copyright © 2022 Nicholas Bojor.

The code in this repository is licensed under the MIT license.
