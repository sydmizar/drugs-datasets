# Drugs, Active Pharmaceutical Ingredients, Diseases and Side Effects databases and scripts

Data Descriptor: Drugs, Active Pharmaceutical Ingredients, Diseases and Side Effects databases, a resource for analysis on drugs-illness interaction.

This repository contains the Python script for the data manipulation algorithm described in the paper [_"Drugs, Active Ingredients and Diseases database. Augmenting the resources for analysis on drugs-illness interaction"_](https://XXXX.XXX) by Irene López, César F. Reyes, Israel Reyes, Tania J. Contreras and Lev Guzmán.

## Requirements
The algorithms work for version of Python >  2.7.
Other necessary packages will be added in the installation and it is important to install PostgreSQL for running *get_data_full.py* script.

## Installation
To install the last version of the package in Python run
```
$ sudo apt-get install python3.7 pip3.7
$ pip install pandas
$ pip install SQLAlchemy
```
## Running the Algorithm
Here are the basic steps for using our algorithm :

```
# Insert number 1 via command line for using PostgreSQL database
python get_full_data.py 1
```
```
# Insert number 2 via command line for using Pandas Dataframes
python get_full_data.py 2
```
Here are the basic steps for using the Jupyter Notebook :
* Click on the Jupyter notebook called *Technical Validation.ipynb*.
* You can download the notebook and run it in your computer or open it on Nbviewer http://nbviewer.ipython.org/.


