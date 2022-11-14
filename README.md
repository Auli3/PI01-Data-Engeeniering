# PI01-Data-Engeeniering

Project Summary

In this project I was tasked to raise various files with diverse extensions,
transformn and load them to a database for future use.
Having in mind an incremental load of the prices files.

I used the following tools:

- Jupyter notebook, to check the data and test transformations.
- Python, used language.
- Pandas, library used for the organization and transformation of the files through Dataframes.
- Numpy, library used for the transformation and manipulation of big volumes of numbers.
- MySQL, database engine used to create and consult the database.
- SQLAlchemy, library used to create the connection with the server, create the database 
with it respective tables and load the data.

ETL flow diagram:

![DER PI 1 drawio](https://user-images.githubusercontent.com/107011436/201666947-1e63d92a-68b2-471c-b989-4f900d43689a.png)

- First from python with pandas I raised the files with different extensions from the
folder "Datasets/Datasets relevamiento precios" and converted them to Dataframes.
- I started by studying the data in the Jupyter Notebook "PI_1.ipynb" to see 
what changes or how should I clean and normalize the data.
- I defined some functions to correct the columns of the price data that will have
a incremental load, and other function to normalize them so that they don't cause any errors when
I load them to the database.
- I worked the other data applying many transformations, specially with the Numpy library
to manage the BigInt data.
- Once all dataframes are normalize except the most recent 
weekly price one, I export them in a same folder with the .csv extension.
- I use the functions created for the normalization of the price data to generate a
pipeline that I saved in another script called "Incremental_load.py".
- I imported SQLAlchemy to connect to my local server of MySQL and create a
database.
- I used the same library to create the destination tables of the data, using the methods
that provide me of every dtype I used.
- I loaded the data of the dataframes worked in Python to the MySQL database.
- Then I used the "Incremental_load.py" script that connects to the database already 
created and does the normalizarion and load of the desired file to MySQL.

Files used:
- PI_1.ipynb, jupyter notebook file where I did the exploration and transformation of the data.
- ETL.py, python script file where I did the process of rainsing the files with 
the data, created the database, transformed and loaded the data to the database.
With the last part of the code destined to do an incremental load of the price table.
- Incremental_load.py, python script file where I created a pipeline destined to
the incremental load of the price table, making sure of keeping the normalization of
the data and its transformations.
database.rar, it's a winrar file with the database backup data.
