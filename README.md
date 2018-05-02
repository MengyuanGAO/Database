## Database Name : gaomy_507project6
#  Databases & Planning


## Instructions

## Part 1 - Creating & adding data to a database

We've provided 3 CSV files: 

* `arkansas.csv`
* `michigan.csv`
* `california.csv`

Each CSV file contains data about National Sites (parks, historical sites, lakeshores, etc) registered with the National Park Service.

First, you should create a database.

* **Sites**

    * ID (SERIAL)
    * Name (VARCHAR up to 128 chars, UNIQUE)
    * Type [e.g. "National Lakeshore" or "National Park"] (VARCHAR up to 128 chars)
    * State_ID (INTEGER - FOREIGN KEY REFERENCING States)
    * Location (VARCHAR up to 255 chars)
    * Description (TEXT)

* **States**

    * ID (SERIAL)
    * Name (VARCHAR up to 40 chars, UNIQUE)


And you should add the data from those `.CSV` files to those database tables as appropriate.

## Part 2  - Making queries to a database

* In Python, query the database for all of the **locations** of the sites. (Of course, this data may vary from "Detroit, Michigan" to "Various States: AL, AK, AR, OH, CA, NV, MD" or the like. That's OK!) Save the resulting data in a variable called `all_locations`.

* In Python, query the database for all of the **names** of the sites whose **descriptions** include the word `beautiful`. Save the resulting data in a variable called `beautiful_sites`.

* In Python, query the database for the total number of **sites whose type is `National Lakeshore`.** Save the resulting data in a variable called `natl_lakeshores`.

* In Python, query your database for the **names of all the national sites in Michigan**. Save the resulting data in a variable called `michigan_names`. You should use an inner join query to do this.

* In Python, query your database for the **total number of sites in Arkansas**. Save the resulting data in a variable called `total_number_arkansas`. You can use multiple queries + Python code to do this, one subquery, or one inner join query. HINT: You'll need to use an aggregate function!

