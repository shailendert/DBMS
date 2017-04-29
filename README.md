CBPDBMS
=================
The first step is to create some data tables and a catalog. Suppose we have a file data.txt with the following contents:

	1,10
	2,20
	3,30
	4,40
	5,50
	5,50

We can convert this into a CBPDBMS table using the convert command:
	
	convert data.txt 2 "int,int"

This creates a file data.dat. In addition to the table's raw data, the two additional parameters specify that each record has two fields and that their types are `INT` and `INT`.

Next, create a catalog file, catalog.txt, with the follow contents:

	data (f1 int, f2 int)	

This tells cbpdbms that there is one table, data (stored in data.dat) with two integer fields named f1 and f2.

Finally, invoke the parser. We must run java from the command line.From the cbpdbms/ directory, type:

	parser catalog.txt

	Added table : data with schema f1(INT)
	f2(INT)

	Computing table stats.
	Done.
	cbpdbms>

Finally, you can run a query:

	SimpleDB> select * from data;
	Started a new transaction tid = 1
	Added scan of table data
	Added select list field null.*
	data.f1	data.f2	
	------------------------
	1	10

	2	20

	3	30

	4	40

	5	50


	 5 rows.
	----------------
	0.04 seconds


And you can also use `-f` to debug in the eclipse to run a query.

Create a query.txt file in cbpdbms/ directory with the following contents:

	select f1 from data;

Run query in eclipse 

	parser catalog.txt -f query.txt

The result in console

	Added table : data with schema f1(INT)
	f2(INT)

	Computing table stats.
	Done.
	Added scan of table data
	Added select list field null.f1
	data.f1	
	------------
	1

	2

	3

	4

	5


	 5 rows.
 
