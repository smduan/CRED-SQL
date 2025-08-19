from openai import OpenAI
import time
import json
import pandas as pd
import re

client = OpenAI(
    api_key="",
)

def completion(prompt):
    max_retries = 100
    retry_count = 0
    result = {}

    client = OpenAI(
    api_key="",

)

    while retry_count < max_retries:
        try:
            start_time = time.time()
            chat_completion = client.chat.completions.create(
                messages=[
                    {
                        'role': 'user',
                        'content': prompt,
                    }
                ],
                model="gpt-4o-2024-08-06",
                temperature=0,
                max_tokens=2000,

            )
            end_time = time.time()
            
            
            result['response_time'] = end_time - start_time
            
            
            result['prompt_tokens'] = chat_completion.usage.prompt_tokens
            result['completion_tokens'] = chat_completion.usage.completion_tokens
            result['total_tokens'] = chat_completion.usage.total_tokens
            
            
            result['content'] = chat_completion.choices[0].message.content
            
            
            break

        except Exception as e:
            print(f"An error occurred: {e}")
            retry_count += 1
            time.sleep(5)
            print(f"Retrying... (attempt {retry_count}/{max_retries})")
            
    return result



import json
with open("spider_EDL_dev.json") as f:
    data = json.load(f)

data_new=[]
#index = 0
for index in range(0,len(data)):#len(data)
    print(f"--- index: {index} ---")
    q_to_edl_prompt='''Given SQLite database schema and question, output EDL, which describes the SQL statement generation according to the execution order of each clause and rules below.\nFour Rules:\n1.Use [] to emphasize database table names and column names.\n2.Generally, the sql explanation starts with scaning tables, then each step builds upon the results of the previous step, and explanation ends with selecting columns.\n3.When there is an sql collection operation UNION,EXCEPT or INTERSECT, describe each subqueries, and the collection operation is described finally.\n4.All descriptions should be as concise as possible.\n
Ten examples are shown below, for your reference:
####Example 1
Database schema:
CREATE TABLE actor (
  actor_id SMALLINT UNSIGNED NOT NULL,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (actor_id)
);
CREATE TABLE address (
  address_id SMALLINT UNSIGNED NOT NULL,
  address VARCHAR(50) NOT NULL,
  address2 VARCHAR(50) DEFAULT NULL,
  district VARCHAR(20) NOT NULL,
  city_id SMALLINT UNSIGNED NOT NULL,
  postal_code VARCHAR(10) DEFAULT NULL,
  phone VARCHAR(20) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (address_id),
  FOREIGN KEY (city_id) REFERENCES city (city_id)
);
CREATE TABLE category (
  category_id TINYINT UNSIGNED NOT NULL,
  name VARCHAR(25) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (category_id)
);
CREATE TABLE city (
  city_id SMALLINT UNSIGNED NOT NULL,
  city VARCHAR(50) NOT NULL,
  country_id SMALLINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (city_id),
  FOREIGN KEY (country_id) REFERENCES country (country_id)
);
CREATE TABLE country (
  country_id SMALLINT UNSIGNED NOT NULL,
  country VARCHAR(50) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (country_id)
);
CREATE TABLE customer (
  customer_id SMALLINT UNSIGNED NOT NULL,
  store_id TINYINT UNSIGNED NOT NULL,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  email VARCHAR(50) DEFAULT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  create_date DATETIME NOT NULL,
  last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (customer_id),
  FOREIGN KEY (address_id) REFERENCES address (address_id),
  FOREIGN KEY (store_id) REFERENCES store (store_id)
);
CREATE TABLE film (
  film_id SMALLINT UNSIGNED NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT DEFAULT NULL,
  release_year YEAR DEFAULT NULL,
  language_id TINYINT UNSIGNED NOT NULL,
  original_language_id TINYINT UNSIGNED DEFAULT NULL,
  rental_duration TINYINT UNSIGNED NOT NULL DEFAULT 3,
  rental_rate DECIMAL(4,2) NOT NULL DEFAULT 4.99,
  length SMALLINT UNSIGNED DEFAULT NULL,
  replacement_cost DECIMAL(5,2) NOT NULL DEFAULT 19.99,
  rating DEFAULT 'G',
  special_features DEFAULT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (film_id),
  FOREIGN KEY (language_id) REFERENCES language (language_id),
  FOREIGN KEY (original_language_id) REFERENCES language (language_id)
);
CREATE TABLE film_actor (
  actor_id SMALLINT UNSIGNED NOT NULL,
  film_id SMALLINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (actor_id,film_id),
 FOREIGN KEY (actor_id) REFERENCES actor (actor_id),
  FOREIGN KEY (film_id) REFERENCES film (film_id)
);
CREATE TABLE film_category (
  film_id SMALLINT UNSIGNED NOT NULL,
  category_id TINYINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (film_id, category_id),
  FOREIGN KEY (film_id) REFERENCES film (film_id),
  FOREIGN KEY (category_id) REFERENCES category (category_id)
);
CREATE TABLE film_text (
  film_id SMALLINT NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  PRIMARY KEY  (film_id)
);
CREATE TABLE inventory (
  inventory_id MEDIUMINT UNSIGNED NOT NULL,
  film_id SMALLINT UNSIGNED NOT NULL,
  store_id TINYINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (inventory_id),
  FOREIGN KEY (store_id) REFERENCES store (store_id),
  FOREIGN KEY (film_id) REFERENCES film (film_id)
);
CREATE TABLE language (
  language_id TINYINT UNSIGNED NOT NULL,
  name CHAR(20) NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (language_id)
);
CREATE TABLE payment (
  payment_id SMALLINT UNSIGNED NOT NULL,
  customer_id SMALLINT UNSIGNED NOT NULL,
  staff_id TINYINT UNSIGNED NOT NULL,
  rental_id INT DEFAULT NULL,
  amount DECIMAL(5,2) NOT NULL,
  payment_date DATETIME NOT NULL,
  last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (payment_id),
  FOREIGN KEY (rental_id) REFERENCES rental (rental_id),
  FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
  FOREIGN KEY (staff_id) REFERENCES staff (staff_id)
);
CREATE TABLE rental (
  rental_id INT NOT NULL,
  rental_date DATETIME NOT NULL,
  inventory_id MEDIUMINT UNSIGNED NOT NULL,
  customer_id SMALLINT UNSIGNED NOT NULL,
  return_date DATETIME DEFAULT NULL,
  staff_id TINYINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (rental_id),
  FOREIGN KEY (staff_id) REFERENCES staff (staff_id),
  FOREIGN KEY (inventory_id) REFERENCES inventory (inventory_id),
  FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
);
CREATE TABLE staff (
  staff_id TINYINT UNSIGNED NOT NULL,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  picture BLOB DEFAULT NULL,
  email VARCHAR(50) DEFAULT NULL,
  store_id TINYINT UNSIGNED NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  username VARCHAR(16) NOT NULL,
  password VARCHAR(40) DEFAULT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (staff_id),
  FOREIGN KEY (store_id) REFERENCES store (store_id),
  FOREIGN KEY (address_id) REFERENCES address (address_id)
);
CREATE TABLE store (
  store_id TINYINT UNSIGNED NOT NULL,
  manager_staff_id TINYINT UNSIGNED NOT NULL,
  address_id SMALLINT UNSIGNED NOT NULL,
  last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY  (store_id),
  FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id),
  FOREIGN KEY (address_id) REFERENCES address (address_id)
);
Question:
What are the payment dates for any payments that have an amount greater than 10 or were handled by a staff member with the first name Elsa?
Output EDL:
#1.Scan Table: Retrieve all rows from the [payment] table.#2.Reserve rows of #1 where the [amount] is greater than 10.#3.Select Column: Select the [payment_date] from the [payment] table from the result of #2 as the first query result.#4.Scan Table: Retrieve all rows from the [payment] table aliased as T1.#5.Join the [staff] table (T2) on the condition that T1.staff_id equals T2.staff_id.#6.Reserve rows of #5 where [first_name] in the T2 table is 'Elsa'.#7.Select the [payment_date] column from the T1 table from the result of #6 as the second query result.#8.Apply UNION operation: Include the results in #3 in the results in #7.

####Example 2
Database schema:
CREATE TABLE "book_club" (
"book_club_id" int,
"Year" int,
"Author_or_Editor" text,
"Book_Title" text,
"Publisher" text,
"Category" text,
"Result" text,
PRIMARY KEY ("book_club_id")
);
CREATE TABLE "movie" (
"movie_id" int,
"Title" text,
"Year" int,
"Director" text,
"Budget_million" real,
"Gross_worldwide" int,
PRIMARY KEY("movie_id")
);
CREATE TABLE "culture_company" (
"Company_name" text,
"Type" text,
"Incorporated_in" text,
"Group_Equity_Shareholding" real,
"book_club_id" text,
"movie_id" text,
PRIMARY KEY("Company_name"),
FOREIGN KEY ("book_club_id") REFERENCES "book_club"("book_club_id"),
FOREIGN KEY ("movie_id") REFERENCES "movie"("movie_id")
);
Question:
Which directors had a movie both in the year 1999 and 2000?
Output EDL:
#1.Scan Table: Retrieve all rows from the [movie] table.#2.Reserve rows of #1 where [YEAR] equals 2000.#3.Select Column: Select the [director] column from the [movie] table from the result of #2 as the first query result.#4.Scan Table: Retrieve all rows from the [movie] table.#5.Reserve rows of #4 where [YEAR] equals 1999.#6.Select Column: Select the [director] column from the [movie] table from the result of #5 as the second query result.#7.Apply INTERSECT operation: Include the results in #6 in the results in #3.

####Example 3
Database schema:
CREATE TABLE station (
    id INTEGER PRIMARY KEY,
    name TEXT,
    lat NUMERIC,
    long NUMERIC,
    dock_count INTEGER,
    city TEXT,
    installation_date TEXT);
CREATE TABLE status (
    station_id INTEGER,
    bikes_available INTEGER,
    docks_available INTEGER,
    time TEXT,
    FOREIGN KEY (station_id) REFERENCES station(id)
);
CREATE TABLE trip (
    id INTEGER PRIMARY KEY,
    duration INTEGER,
    start_date TEXT,
    start_station_name TEXT, -- this should be removed
    start_station_id INTEGER,
    end_date TEXT,
    end_station_name TEXT, -- this should be removed
    end_station_id INTEGER,
    bike_id INTEGER,
    subscription_type TEXT,
    zip_code INTEGER);
CREATE TABLE weather (
    date TEXT,
    max_temperature_f INTEGER,
    mean_temperature_f INTEGER,
    min_temperature_f INTEGER,
    max_dew_point_f INTEGER,
    mean_dew_point_f INTEGER,
    min_dew_point_f INTEGER,
    max_humidity INTEGER,
    mean_humidity INTEGER,
    min_humidity INTEGER,
    max_sea_level_pressure_inches NUMERIC,
    mean_sea_level_pressure_inches NUMERIC,
    min_sea_level_pressure_inches NUMERIC,
    max_visibility_miles INTEGER,
    mean_visibility_miles INTEGER,
    min_visibility_miles INTEGER,
    max_wind_Speed_mph INTEGER,
    mean_wind_speed_mph INTEGER,
    max_gust_speed_mph INTEGER,
    precipitation_inches INTEGER,
    cloud_cover INTEGER,
    events TEXT,
    wind_dir_degrees INTEGER,
    zip_code INTEGER);
Question:
What is the mean longitude for all stations that have never had more than 10 bikes available?
Output EDL:
#1.Scan Table: Retrieve all rows from the [station] table.#2.Scan Table: Retrieve all rows from the [status] table in a subquery.#3.Group the results of #2 by the [station_id] column.#4.Apply Having Clause: Reserve the grouped rows of #3 where the maximum of the [bikes_available] column is greater than 10.#5.Select Column: Select the [station_id] column from the [status] table from the result of #4.#6.Reserve rows of #1 where [id] is not included in the result of #5.#7.Select Column: Select the average of the [long] column from the [station] table from the result of #6.

####Example 4
Database schema:
CREATE TABLE "city" (
"City_ID" int,
"City" text,
"Hanzi" text,
"Hanyu_Pinyin" text,
"Regional_Population" int,
"GDP" real,
PRIMARY KEY ("City_ID")
);
CREATE TABLE "match" (
"Match_ID" int,
"Date" text,
"Venue" text,
"Score" text,
"Result" text,
"Competition" text,
PRIMARY KEY ("Match_ID")
);
CREATE TABLE "temperature" (
"City_ID" int,
"Jan" real,
"Feb" real,
"Mar" real,
"Apr" real,
"Jun" real,
"Jul" real,
"Aug" real,
"Sep" real,
"Oct" real,
"Nov" real,
"Dec" real,
PRIMARY KEY ("City_ID"),
FOREIGN KEY (`City_ID`) REFERENCES `city`(`City_ID`)
);
CREATE TABLE "hosting_city" (
  "Year" int,
  "Match_ID" int,
  "Host_City" text,
  PRIMARY KEY ("Year"),
  FOREIGN KEY (`Host_City`) REFERENCES `city`(`City_ID`),
  FOREIGN KEY (`Match_ID`) REFERENCES `match`(`Match_ID`)
)
Question:
Which cities have higher temperature in Feb than in Jun or have once served as host cities?
Output EDL:
#1. Scan Table: Retrieve all rows from the [city] table aliased as T1.#2. Scan Table: Retrieve all rows from the [temperature] table aliased as T2.#3. Join the [temperature] table aliased as T2 on the condition that T1.city_id equals T2.city_id.#4. Reserve rows of #3 where the [Feb] column in table T2 is greater than the [Jun] column in table T2.#5. Select Column: Select the [city] column from the [T1] table from the result of #4 as the first query result.#6. Scan Table: Retrieve all rows from the [city] table aliased as T3.#7. Scan Table: Retrieve all rows from the [hosting_city] table aliased as T4.#8. Join the [hosting_city] table aliased as T4 on the condition that T3.city_id equals T4.host_city.#9. Select Column: Select the [city] column from the [T3] table from the result of #8 as the second query result.#10. Apply UNION operation: Include the results in #9 in the results in #5.

####Example 5
Database schema:
CREATE TABLE `Addresses` (
`address_id` INTEGER PRIMARY KEY,
`address_content` VARCHAR(80),
`city` VARCHAR(50),
`zip_postcode` VARCHAR(20),
`state_province_county` VARCHAR(50),
`country` VARCHAR(50),
`other_address_details` VARCHAR(255)
);
CREATE TABLE `Products` (
`product_id` INTEGER PRIMARY KEY,
`product_details` VARCHAR(255)
);
INSERT INTO Products (`product_id`, `product_details`) VALUES (1, 'Americano');
INSERT INTO Products (`product_id`, `product_details`) VALUES (2, 'Dove Chocolate');
INSERT INTO Products (`product_id`, `product_details`) VALUES (3, 'Latte');
CREATE TABLE `Customers` (
`customer_id` INTEGER PRIMARY KEY,
`payment_method` VARCHAR(15) NOT NULL,
`customer_name` VARCHAR(80),
`date_became_customer` DATETIME,
`other_customer_details` VARCHAR(255)
);
CREATE TABLE `Customer_Addresses` (
`customer_id` INTEGER NOT NULL,
`address_id` INTEGER NOT NULL,
`date_address_from` DATETIME NOT NULL,
`address_type` VARCHAR(15) NOT NULL,
`date_address_to` DATETIME,
FOREIGN KEY (`address_id` ) REFERENCES `Addresses`(`address_id` ),
FOREIGN KEY (`customer_id` ) REFERENCES `Customers`(`customer_id` )
);
CREATE TABLE `Customer_Contact_Channels` (
`customer_id` INTEGER NOT NULL,
`channel_code` VARCHAR(15) NOT NULL,
`active_from_date` DATETIME NOT NULL,
`active_to_date` DATETIME,
`contact_number` VARCHAR(50) NOT NULL,
FOREIGN KEY (`customer_id` ) REFERENCES `Customers`(`customer_id` )
);
CREATE TABLE `Customer_Orders` (
`order_id` INTEGER PRIMARY KEY,
`customer_id` INTEGER NOT NULL,
`order_status` VARCHAR(15) NOT NULL,
`order_date` DATETIME,
`order_details` VARCHAR(255),
FOREIGN KEY (`customer_id` ) REFERENCES `Customers`(`customer_id` )
);
CREATE TABLE `Order_Items` (
`order_id` INTEGER NOT NULL,
`product_id` INTEGER NOT NULL,
`order_quantity` VARCHAR(15),
FOREIGN KEY (`product_id` ) REFERENCES `Products`(`product_id` ),
FOREIGN KEY (`order_id` ) REFERENCES `Customer_Orders`(`order_id` )
);
Question:
How many types of products have Rodrick Heaney bought in total?
Output EDL:
#1.Scan Table: Retrieve all rows from the [customers] table aliased as [t1].#2.Join the [customer_orders] table aliased as [t2] on the condition that t1.customer_id equals t2.customer_id.#3.Join the [order_items] table aliased as [t3] on the condition that t2.order_id equals t3.order_id.#4.Reserve rows of #3 where t1.customer_name equals 'Rodrick Heaney'.#5.Select Column: Select the count of distinct [product_id] from the [t3] table from the result of #4.

####Example 6
Database schema:
CREATE TABLE "architect" (
"id" text,
"name" text,
"nationality" text,
"gender" text,
primary key("id")
);
CREATE TABLE "bridge" (
"architect_id" int,
"id" int,
"name" text,
"location" text,
"length_meters" real,
"length_feet" real,
primary key("id"),
foreign key ("architect_id" ) references `architect`("id")
);
CREATE TABLE "mill" (
"architect_id" int,
"id" int,
"location" text,
"name" text,
"type" text,
"built_year" int,
"notes" text,
primary key ("id"),
foreign key ("architect_id" ) references `architect`("id")
);
Question:
What is the id, name and nationality of the architect who built most mills?
Output EDL:
#1.Scan Table: Retrieve all rows from the [architect] table aliased as T1.#2.Join: Join the [mill] table aliased as T2 on the condition that T1.id equals T2.architect_id.#3.Group: Group #2 by the [id] column in the T1 table.#4.Sort Row: Order #3 by the count of rows in descending order.#5.Limit: Limit #4 to the top 1 record.#6.Select Column: Select the [id], [name], and [nationality] columns from the [T1] table in the result of #5.

####Example 7
Database schema:
CREATE TABLE `Accounts` (
`account_id` INTEGER PRIMARY KEY,
`customer_id` INTEGER NOT NULL,
`account_name` VARCHAR(50),
`other_account_details` VARCHAR(255)
);
CREATE TABLE `Customers` (
`customer_id` INTEGER PRIMARY KEY,
`customer_first_name` VARCHAR(20),
`customer_last_name` VARCHAR(20),
`customer_address` VARCHAR(255),
`customer_phone` VARCHAR(255),
`customer_email` VARCHAR(255),
`other_customer_details` VARCHAR(255)
);
CREATE TABLE `Customers_Cards` (
`card_id` INTEGER PRIMARY KEY,
`customer_id` INTEGER NOT NULL,
`card_type_code` VARCHAR(15) NOT NULL,
`card_number` VARCHAR(80),
`date_valid_from` DATETIME,
`date_valid_to` DATETIME,
`other_card_details` VARCHAR(255)
);
CREATE TABLE `Financial_Transactions` (
`transaction_id` INTEGER NOT NULL ,
`previous_transaction_id` INTEGER,
`account_id` INTEGER NOT NULL,
`card_id` INTEGER NOT NULL,
`transaction_type` VARCHAR(15) NOT NULL,
`transaction_date` DATETIME,
`transaction_amount` DOUBLE NULL,
`transaction_comment` VARCHAR(255),
`other_transaction_details` VARCHAR(255),
FOREIGN KEY (`card_id` ) REFERENCES `Customers_Cards`(`card_id` ),
FOREIGN KEY (`account_id` ) REFERENCES `Accounts`(`account_id` )
);
Question:
Show distinct first and last names for all customers with an account.
Output EDL:
#1.Scan Table: Retrieve all rows from the [Customers] table aliased as [T1].#2.Join the [Accounts] table aliased as [T2] on the condition that T1.customer_id equals T2.customer_id.#3.Select Column: Select the distinct [customer_first_name] and [customer_last_name] columns from the [T1] table from the result of #2.

####Example 8
Database schema:
CREATE TABLE "city" (
"City_ID" int,
"Official_Name" text,
"Status" text,
"Area_km_2" real,
"Population" real,
"Census_Ranking" text,
PRIMARY KEY ("City_ID")
);
CREATE TABLE "farm" (
"Farm_ID" int,
"Year" int,
"Total_Horses" real,
"Working_Horses" real,
"Total_Cattle" real,
"Oxen" real,
"Bulls" real,
"Cows" real,
"Pigs" real,
"Sheep_and_Goats" real,
PRIMARY KEY ("Farm_ID")
);
CREATE TABLE "farm_competition" (
"Competition_ID" int,
"Year" int,
"Theme" text,
"Host_city_ID" int,
"Hosts" text,
PRIMARY KEY ("Competition_ID"),
FOREIGN KEY (`Host_city_ID`) REFERENCES `city`(`City_ID`)
);
CREATE TABLE "competition_record" (
"Competition_ID" int,
"Farm_ID" int,
"Rank" int,
PRIMARY KEY ("Competition_ID","Farm_ID"),
FOREIGN KEY (`Competition_ID`) REFERENCES `farm_competition`(`Competition_ID`),
FOREIGN KEY (`Farm_ID`) REFERENCES `farm`(`Farm_ID`)
);
Question:
Show the official names of the cities that have hosted more than one competition.
Output EDL:
#1.Scan Table: Retrieve all rows from the [head] table.#2.Group #1 by [born_state].#3.Apply Having Clause: Reserve the grouped rows of #2 where the count of rows is 3 or more.#4.Select Column: Select [born_state] from the [head] table from the result of #3.

####Example 9
Database schema:
create table flight(
	flno number(4,0) primary key,
	origin varchar2(20),
	destination varchar2(20),
	distance number(6,0),
	departure_date date,
	arrival_date date,
	price number(7,2),
    aid number(9,0),
    foreign key("aid") references `aircraft`("aid"));
create table aircraft(
	aid number(9,0) primary key,
	name varchar2(30),
	distance number(6,0));
create table employee(
	eid number(9,0) primary key,
	name varchar2(30),
	salary number(10,2));
create table certificate(
	eid number(9,0),
	aid number(9,0),
	primary key(eid,aid),
	foreign key("eid") references `employee`("eid"),
	foreign key("aid") references `aircraft`("aid"));
Question:
What is the count of aircrafts that have a distance between 1000 and 5000?
Output EDL:
#1.Scan Table: Retrieve all rows from the [Aircraft] table.#2.Reserve rows of #1 where [distance] is between 1000 and 5000.#3.Select Column: Select the count of rows from the [Aircraft] table from the result of #2.

####Example 10
Database schema:
CREATE TABLE Customers (
Customer_ID INTEGER NOT NULL,
Customer_name VARCHAR(40),
PRIMARY KEY (Customer_ID)
);
CREATE TABLE Services (
Service_ID INTEGER NOT NULL,
Service_name VARCHAR(40),
PRIMARY KEY (Service_ID)
);
CREATE TABLE Available_Policies (
Policy_ID INTEGER NOT NULL,
policy_type_code CHAR(15),
Customer_Phone VARCHAR(255),
PRIMARY KEY (Policy_ID),
UNIQUE (Policy_ID)
);
CREATE TABLE Customers_Policies (
Customer_ID INTEGER NOT NULL,
Policy_ID INTEGER NOT NULL,
Date_Opened DATE,
Date_Closed DATE,
PRIMARY KEY (Customer_ID, Policy_ID),
FOREIGN KEY (Customer_ID) REFERENCES Customers (Customer_ID),
FOREIGN KEY (Policy_ID) REFERENCES Available_Policies (Policy_ID)
);
CREATE TABLE First_Notification_of_Loss (
FNOL_ID INTEGER NOT NULL,
Customer_ID INTEGER NOT NULL,
Policy_ID INTEGER NOT NULL,
Service_ID INTEGER NOT NULL,
PRIMARY KEY (FNOL_ID),
UNIQUE (FNOL_ID),
FOREIGN KEY (Service_ID) REFERENCES Services (Service_ID),
FOREIGN KEY (Customer_ID, Policy_ID) REFERENCES Customers_Policies (Customer_ID,Policy_ID)
);
CREATE TABLE Claims (
Claim_ID INTEGER NOT NULL,
FNOL_ID INTEGER NOT NULL,
Effective_Date DATE,
PRIMARY KEY (Claim_ID),
UNIQUE (Claim_ID),
FOREIGN KEY (FNOL_ID) REFERENCES First_Notification_of_Loss (FNOL_ID)
);
CREATE TABLE Settlements (
Settlement_ID INTEGER NOT NULL,
Claim_ID INTEGER,
Effective_Date DATE,
Settlement_Amount REAL,
PRIMARY KEY (Settlement_ID),
UNIQUE (Settlement_ID),
FOREIGN KEY (Claim_ID) REFERENCES Claims (Claim_ID)
);
Question:
Retrieve the open and close dates of all the policies associated with the customer whose name contains "Diana"
Output EDL:
#1.Scan Table: Retrieve all rows from the [customers] table (t1).#2.Join the [customers_policies] table (t2) on the condition that t1.customer_id equals t2.customer_id.#3.Reserve rows of #2 where the [customer_name] in table t1 contains \"Diana\".#4.Select Column: Select the [date_opened] and [date_closed] columns from the [t2] table from the result of #3.
    '''
    start = data[index]["instruction"].find("All descriptions should be as concise as possible.\n\nDatabase schema:")+len("All descriptions should be as concise as possible.\n\nDatabase schema:")
    end = data[index]["instruction"].find("\nQuestion:")
    ori_schema = data[index]["instruction"][start:end]


    start = data[index]["instruction"].find("\nQuestion:")+len("\nQuestion:")
    end = data[index]["instruction"].find("\nOutput description:")
    question = data[index]["instruction"][start:end]


    q_to_edl_instruction = q_to_edl_prompt+'Now is your turn.\nDatabase schema:\n'+ori_schema+"\nQuestion:\n"+question+"\nOutput EDL:\n"
    q_to_edl_response = completion(q_to_edl_instruction)
    generate_edl = q_to_edl_response['content'].replace("\n","")
    print(generate_edl)
    print(f"Response Time: {q_to_edl_response['response_time']:.2f} seconds")

    edl_to_sql_prompt = '''You are a SQL expert. Given EDL, which is a formalism to describe data retrieval operations over a SQL schema in a modular manner, output final sqlite SQL query.
Ten examples are shown below, for your reference:
####Example 1
EDL:
#1.Scan Table: Retrieve all rows from the [payment] table.#2.Reserve rows of #1 where the [amount] is greater than 10.#3.Select Column: Select the [payment_date] from the [payment] table from the result of #2 as the first query result.#4.Scan Table: Retrieve all rows from the [payment] table aliased as T1.#5.Join the [staff] table (T2) on the condition that T1.staff_id equals T2.staff_id.#6.Reserve rows of #5 where [first_name] in the T2 table is 'Elsa'.#7.Select the [payment_date] column from the T1 table from the result of #6 as the second query result.#8.Apply UNION operation: Include the results in #3 in the results in #7.
Only output SQL without any other text:
SELECT payment_date FROM payment WHERE amount  >  10 UNION SELECT T1.payment_date FROM payment AS T1 JOIN staff AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.first_name  =  'Elsa'

####Example 2
EDL:
#1.Scan Table: Retrieve all rows from the [movie] table.#2.Reserve rows of #1 where [YEAR] equals 2000.#3.Select Column: Select the [director] column from the [movie] table from the result of #2 as the first query result.#4.Scan Table: Retrieve all rows from the [movie] table.#5.Reserve rows of #4 where [YEAR] equals 1999.#6.Select Column: Select the [director] column from the [movie] table from the result of #5 as the second query result.#7.Apply INTERSECT operation: Include the results in #6 in the results in #3.
Only output SQL without any other text:
SELECT director FROM movie WHERE YEAR  =  2000 INTERSECT SELECT director FROM movie WHERE YEAR  =  1999

####Example 3
EDL:
#1.Scan Table: Retrieve all rows from the [station] table.#2.Scan Table: Retrieve all rows from the [status] table in a subquery.#3.Group the results of #2 by the [station_id] column.#4.Apply Having Clause: Reserve the grouped rows of #3 where the maximum of the [bikes_available] column is greater than 10.#5.Select Column: Select the [station_id] column from the [status] table from the result of #4.#6.Reserve rows of #1 where [id] is not included in the result of #5.#7.Select Column: Select the average of the [long] column from the [station] table from the result of #6.
Only output SQL without any other text:
SELECT avg(long) FROM station WHERE id NOT IN (SELECT station_id FROM status GROUP BY station_id HAVING max(bikes_available)  >  10)

####Example 4
EDL:
#1. Scan Table: Retrieve all rows from the [city] table aliased as T1.#2. Scan Table: Retrieve all rows from the [temperature] table aliased as T2.#3. Join the [temperature] table aliased as T2 on the condition that T1.city_id equals T2.city_id.#4. Reserve rows of #3 where the [Feb] column in table T2 is greater than the [Jun] column in table T2.#5. Select Column: Select the [city] column from the [T1] table from the result of #4 as the first query result.#6. Scan Table: Retrieve all rows from the [city] table aliased as T3.#7. Scan Table: Retrieve all rows from the [hosting_city] table aliased as T4.#8. Join the [hosting_city] table aliased as T4 on the condition that T3.city_id equals T4.host_city.#9. Select Column: Select the [city] column from the [T3] table from the result of #8 as the second query result.#10. Apply UNION operation: Include the results in #9 in the results in #5.
Only output SQL without any other text:
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Feb  >  T2.Jun UNION SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city

####Example 5
EDL:
#1.Scan Table: Retrieve all rows from the [customers] table aliased as [t1].#2.Join the [customer_orders] table aliased as [t2] on the condition that t1.customer_id equals t2.customer_id.#3.Join the [order_items] table aliased as [t3] on the condition that t2.order_id equals t3.order_id.#4.Reserve rows of #3 where t1.customer_name equals 'Rodrick Heaney'.#5.Select Column: Select the count of distinct [product_id] from the [t3] table from the result of #4.
Only output SQL without any other text:
SELECT count(DISTINCT t3.product_id) FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id WHERE t1.customer_name  =  'Rodrick Heaney'

####Example 6
EDL:
#1.Scan Table: Retrieve all rows from the [architect] table aliased as T1.#2.Join: Join the [mill] table aliased as T2 on the condition that T1.id equals T2.architect_id.#3.Group: Group #2 by the [id] column in the T1 table.#4.Sort Row: Order #3 by the count of rows in descending order.#5.Limit: Limit #4 to the top 1 record.#6.Select Column: Select the [id], [name], and [nationality] columns from the [T1] table in the result of #5.
Only output SQL without any other text:
SELECT T1.id ,  T1.name ,  T1.nationality FROM architect AS T1 JOIN mill AS T2 ON T1.id  =  T2.architect_id GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1

####Example 7
EDL:
#1.Scan Table: Retrieve all rows from the [Customers] table aliased as [T1].#2.Join the [Accounts] table aliased as [T2] on the condition that T1.customer_id equals T2.customer_id.#3.Select Column: Select the distinct [customer_first_name] and [customer_last_name] columns from the [T1] table from the result of #2.
Only output SQL without any other text:
SELECT DISTINCT T1.customer_first_name ,  T1.customer_last_name FROM Customers AS T1 JOIN Accounts AS T2 ON T1.customer_id  =  T2.customer_id

####Example 8
EDL:
#1.Scan Table: Retrieve all rows from the [head] table.#2.Group #1 by [born_state].#3.Apply Having Clause: Reserve the grouped rows of #2 where the count of rows is 3 or more.#4.Select Column: Select [born_state] from the [head] table from the result of #3.
Only output SQL without any other text:
SELECT born_state FROM head GROUP BY born_state HAVING count(*)  >=  3

####Example 9
EDL:
#1.Scan Table: Retrieve all rows from the [Aircraft] table.#2.Reserve rows of #1 where [distance] is between 1000 and 5000.#3.Select Column: Select the count of rows from the [Aircraft] table from the result of #2.
Only output SQL without any other text:
SELECT count(*) FROM Aircraft WHERE distance BETWEEN 1000 AND 5000

####Example 10
EDL:
#1.Scan Table: Retrieve all rows from the [customers] table (t1).#2.Join the [customers_policies] table (t2) on the condition that t1.customer_id equals t2.customer_id.#3.Reserve rows of #2 where the [customer_name] in table t1 contains \"Diana\".#4.Select Column: Select the [date_opened] and [date_closed] columns from the [t2] table from the result of #3.
Only output SQL without any other text:
SELECT t2.date_opened ,  t2.date_closed FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.customer_name LIKE \"%Diana%\"
    '''

    
    edl_to_sql_instruction = edl_to_sql_prompt+'Now is your turn.\nEDL:\n'+generate_edl+"\nOnly output SQL without any other text:\n"
    edl_to_sql_response = completion(edl_to_sql_instruction)
    generate_sql = edl_to_sql_response['content'].replace("\n"," ")
    print(generate_sql)
    print(f"Response Time: {edl_to_sql_response['response_time']:.2f} seconds")



    item = {"index":index,"question":question,"generate_edl":q_to_edl_response['content'].replace("\n",""),'gold_edl':data[index]["output"],'generate_sql':generate_sql,"q_to_edl_instruction": q_to_edl_instruction,"q_to_edl_response":q_to_edl_response,"edl_to_sql_instruction": edl_to_sql_instruction,"edl_to_sql_response":edl_to_sql_response,'ori_schema':ori_schema}
    
    data_new.append(item)
    
    with open("result_q_edl_sql.json","w") as f:
        json.dump(data_new, f, ensure_ascii=False,indent=4)
    #index = index+1
generate_edl_sql_results = [item["generate_sql"] + '\n' for item in data_new]    

with open('generate_edl_sql_results.txt', 'w', encoding='utf-8') as f:
    f.writelines(generate_edl_sql_results)  