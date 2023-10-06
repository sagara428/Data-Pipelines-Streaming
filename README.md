# Batch Processing with PySpark

This project demonstrates batch processing with PySpark using docker which contains the following containers.

## Table of Contents
- [Dataset](#dataset)
- [How to run](#how-to-run)
- [Docker troubleshooting](#docker-troubleshooting)
- [Spark Scripts](#sparkscripts)
- [Analysis result](#analysis-result)



## Dataset
The dataset used in this project is `online-retail-dataset` which is from UCI Machine Learning Repository.
The dataset is a real online retail transaction dataset about a UK-based non-store online retail company which mainly sells unique all-occasion gift-ware.

### Dataset Information

- **InvoiceNo**: Invoice number. Nominal. A 6-digit integral number uniquely assigned to each transaction. If this code starts with the letter 'c', it indicates a cancellation.

- **StockCode**: Product (item) code. Nominal. A 5-digit integral number uniquely assigned to each distinct product.

- **Description**: Product (item) name. Nominal.

- **Quantity**: The quantities of each product (item) per transaction. Numeric.

- **InvoiceDate**: Invoice date and time. Numeric. The day and time when a transaction was generated.

- **UnitPrice**: Unit price. Numeric. Product price per unit in sterling.

- **CustomerID**: Customer number. Nominal. A 5-digit integral number uniquely assigned to each customer.

- **Country**: Country name. Nominal. The name of the country where a customer resides.


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

## Spark Scripts
The structure of the Spark Scripts folder is like this.
- spark-scripts
  - main.py
  - lib
    - extract.py
    - transform.py
    - load.py
    - __init__.py
  - jar
    - postgresql-42.6.0.jar

- The jar file is to help spark connect to postgreSQL.
- Here is the description on `main.py`: 
    ![main](/img/main.png)
- Here is the description on `extract.py`:
    ![extract](/img/extract.png)
- Here is the description on `transform.py`:
    ![transform](/img/transform.png)
- Here is the description on `load.py`:
    ![load](/img/load.png)

## ETL Result
### Adding `is_cancelled` Column
Here, the retail dataset is enriched by adding `is_cancelled` column to make it easier to further analyze the cancelled transaction for future projects whether for machine learning, like customer segmentation or maybe sentiment analysis with the help of analyzing `is_cancelled` column and `description` column. Here in this project, I used this column to find completion rate.

### Data Cleaning
For the data cleaning, after checking the datasets, there are some interesting things:
- Quantity has negative values.
- Only cancelled transaction has negative value on quantity. This might suggest that negative value quantities means returned products due to cancelled transactions.
- Transaction with negative quantity has description which is not the name of the product, but something that might describe why the item is returned.
- There are transactions of product with 0 unit price. This might be a bonus from a certain transaction or something.
- There is a unit price with negative value.
- There are customers without customerId (Null).
- There are Null descriptions and all transaction with Null description has Null customerId (Ghost customers).
- There is no Null in other columns.

Based on those informations, the cleaning is done by removing all rows Null customerId first because those rows cannot be used for more exploration like by joining with another datasets and cannot be used for finding rate related metrics which measure the performance of customers. Transactions with 0 unit price is also removed because it does not add any valuable insight in customers or the company's performance analysis. After removing those two, surprisingly transactions with unit price of negative value is already removed. Here is the result of data cleaning:
![Cleaned Retail](/img/postgres-retail-cleaned.png)

### Completion Rate

Here, completion rate is a metric that measure the performance of the online retail company to make succesful transaction in each country. The value is calculated by finding the ratio of succesful transaction to total transaction (cancelled transactions included).
![completion rate](/img/postgres-completion-rate.png)

Surprisingly, there are about 9 countries in UK in which the company does not experience cancelled transactions. It might be because the products offered by the online retail company, which are unique all-occasion gift-wares, match the preference of people from those countries.

### Monthly Churn Rate
Here, the monthly churn rate is calculated to measure the performance of the company's customers in each month. The churn rate is a rate of customers who stop subscribing or doing transaction on the online retail company which is calculated in a period of time (in this case, monthly).  In calculating churn rate, the data is filtered from cancelled transactions to avoid misinterpretation of customers with cancelled transactions as a churned customers or customers who stop subscribing or doing transactions. The difference is actually that cancellation can be reversed (the customers might want to do transaction without cancelling this time), but churn is forever.
![Churn rate](/img/postgres-churn-rate.png)
Obviously the lower the churn rate, the better. Here, the churn rate of each months hit the lowest value at the first two months, and then increasing and fluctuating around 30 and 40 for a couple of months later. 


