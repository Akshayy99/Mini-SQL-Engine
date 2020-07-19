# Mini-SQL-Engine

A Mini SQL Engine coded for a Database Systems Course at IIIT H.

## Input Format
The tables are given as `.csv` files with the filename as tablename. If file is named `File1.csv`, Table name will be `File1`. Data type are all numbers only!

A `metadata.txt` file also must be provided which has the following structure
  <begin_table>
  <table_name>
  <attribute1>
  .
  .
  <atrributeN>
  <end_table>
(See the example in the repo!)

## Type of Queries

It can handle normal "Select" Queries of following types:
  1. Select All Records : E.g. 
  ```bash
  select * from table_name;
  ```
  2. Aggregate Functions : Simple aggregate functions like count,min,max,avg,sum E.g. 
  ```bash
  select max(col1) from table1;
  ```
  3. Project Columns : Could be any number of columns from one or more tables E.g. 
  ```bash
  select col1,col2 from table_name;
  ```
  4. Select/Project with distinct from one table : E.g. 
  ```bash
  select distinct col1,col2 from table1;
  ```
  5. Select with where from one or more tables(Only AND & OR Operators. Multiple Operators supported!) : E.g. 
  ```bash
  select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;
  ```
  6. Projection of one or more (incl. all) columns from two tables with one join condition : 
      E.g. 
  ```bash
  select * from table1, table2 where table1.col1 = table2.col2;
  ```
  ```bash
  select col1,col2 from table1,table2 where table1.col1 = table2.col2;
  ```
  (Joining columns are printed only once!)

All possible Error Cases are handled!

## To Execute :

```bash
python engine.py "query"
```
## Python Imports :

```bash
csv
sys
re
collections
```


