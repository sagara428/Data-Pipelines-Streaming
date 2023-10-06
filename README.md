# Batch Processing with PySpark

This project demonstrates a Data Pipelines for Streaming System with:
Event Producer:
- Produce purchasing events with faker

Streaming job:
- Listen to kafka topic
- Aggregate for daily total purchase
- Write it to console with these columns: Timestamp, Running total
- Using foreachBatch


## How to run
1. Clone this repository
2. Open terminal / CMD and change the directory to the cloned repository folder
3. For x86 user, run:
    ```console
    make docker-build
    ```
    For arm chip user, edit "docker-compose-airflow.yml" file in "docker" folder by changing `amd64` to `arm64` in all platform settings, then run:
    ```console
    make docker-build-arm
    ```
    ![Make Docker Build](/img/make-docker-build.png)
4. After the setup is complete, run these three commands in order: 

    `make postgres`
    ![Make postgres](/img/make-postgres.png)
    
    `make spark`

    ![Make Spark](/img/make-spark.png)
    `make airflow`
    ![Make Airflow](/img/make-airflow.png)
 
5. Wait until the airflow setup is already, then access airflow Webserver UI in your web browser access: `localhost:8081`
    ![airflow home](/img/airflow-home.png)
    then run the dag by manually trigger it with clicking the small play button at the top right of the UI.
    ![airflow dag](/img/aiflow-dag.png)

6. Since the result of the task is loading a dataset / table into postgreSQL, you can check the table by connecting postgreSQL database we made by using Dbeaver. You can check the postgres setup at `.env `file. 
    ![make postgres1](/img/connect-postgres-1.png)
    ![make postgres2](/img/connect-postgres-2.png)
    ![make postgres3](/img/connect-postgres-3.png)


## Docker Troubleshooting
### `make airflow` error because of `entrypoint.sh`
This error happens because `entrypoint.sh` cannot be detected at the specified path. 
    ![entrypoint1](/img/entrypoint-error-in-CRLF.png)
There are many possible solutions, one of the solution is to open the `entrypoint.sh` at VSCode then change the encoding to UTF-8 if it is not and try to change the end line sequence from CRLF to LF and save the changes. 
    ![entrypoint2](/img/entrypoint-error-in-CRLF-2.png)
    ![entrypoint3](/img/entrypoint-error-in-CRLF-3.png)
Then either run `make airflow` again or restart the setup from `make docker-build` again should solve the problem.

### `dataeng-network already exist` when `make docker-build`
The solution is quite simple, run `docker network rm network-name`:
```console
docker network rm dataeng-network
```
You can also check the lists of network already created by running:
```console
docker network ls
```
Another useful command: `docker network prune` to delete all networks created in docker environment.
After deleting the network, you can start again from `make docker-build`.

### Device becomes so laggy or BSOD occurs when running docker.
If after running the script with docker many times and then you suddenly experiencing BSOD often occurs when starting docker or device becomes so laggy then maybe it is occured because docker is using too much diskspace.
 ![load](/img/docker-disk-space.png)

The following command can help you free up some device and fix the problem:
```console
docker object prune 
```
change the `object` to `image`, `system`, `container`, or `volumes` to prune the unused docker objects and free up some diskspaces. You can check the documentation [here](https://docs.docker.com/config/pruning/).




