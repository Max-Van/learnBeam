# Learn Beam Python MySQL SDK

--------------

##  Beam MySQL SDK


### Install it 

Install it with following command, find more detail from [here](https://pypi.org/project/beam-mysql-connector/)

```bash
pip install beam-mysql-connector
```

### Read from MySQL

```python
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


read_from_mysql = ReadFromMySQL(
        query="SELECT * FROM test_db.tests;",
        host="localhost",
        database="test_db",
        user="test",
        password="test",
        port=3306,
        splitter=splitters.NoSplitter()  # you can select how to split query from splitters
)
```


### Write to MySQL

```python
from beam_mysql.connector.io import WriteToMySQL


write_to_mysql = WriteToMySQL(
        host="localhost",
        database="test_db",
        table="tests",
        user="test",
        password="test",
        port=3306,
        batch_size=1000,
)
```

## My Exercise 

+ Read Data from MySQL table and write it to txt file 
+ **beam.Filter**:  Read Data from MySQL table, filter records and write result to txt file 
+ **beam.ParDo**: Read Data from MySQL table, only extract some fields from it,then write it to txt file  
+ **beam.combiners.Count** and **beam.Map**: Read Data from MySQL table, count records and write result to txt file
+ **beam.combiners.Mean.PerKey** : Read Data from MySQL table, calculate the average of a given filed and write result to txt file

Please find the respective py file and run it as below, the output file will be generated under current path. 

```bash
python file_name.py
```

Please find more detail on [beam transform here](https://beam.apache.org/documentation/transforms/python/overview/)


## Sample Data 

You can build up the mysql database yourself. 
The MySQL sample database schema consists of the following tables:

 
- Customers: stores customer’s data.
- Products: stores a list of scale model cars.
- ProductLines: stores a list of product line categories.
- Orders: stores sales orders placed by customers.
- OrderDetails: stores sales order line items for each sales order.
- Payments: stores payments made by customers based on their accounts.
- Employees: stores all employee information as well as the organization structure such as who reports to whom.
- Offices: stores sales office data.


![alt 属性文本](https://sp.mysqltutorial.org/wp-content/uploads/2009/12/MySQL-Sample-Database-Schema.png "可选标题")

## Reference 


+ You can download and create the [MySQL Sample Data](https://sp.mysqltutorial.org/wp-content/uploads/2018/03/mysqlsampledatabase.zip) 

+ Find more detail on those data [here](https://www.mysqltutorial.org/mysql-sample-database.aspx/)