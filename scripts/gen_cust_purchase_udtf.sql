//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.dt_demo;

--Third table is SALESDATA to store raw product sales by customer and purchase date
create or replace function gen_cust_purchase(num_records number, ndays number)
returns table(custid number(10), purchase variant)
language python
runtime_version=3.10
handler='genCustPurchase'
packages=('Faker')
as
$$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records, ndays):
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)

            customer_purchase = {
                 'custid':c_id
                ,'purchased':[]
            }

            # Get the current date
            current_date = datetime.now()

            # Calculate the maximum date(days from now)
            min_date = current_date - timedelta(days=ndays)

            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date, current_date)

            purchase ={
                'prodid':fake.random_int(min=101, max=199)
               ,'quantity':fake.random_int(min=1, max=5)
               ,'purchase_amount':round(random.uniform(10, 1000),2)
               ,'purchase_date':pdate
            }

            customer_purchase['purchased'].append(purchase)

            yield(c_id, purchase)

$$;

//Validate the function
show functions like 'gen_cust_purchase';

//Create table and insert records 
create or replace table salesdata as select * from table(gen_cust_purchase(10000,10));

//Validate table records
select * from salesdata limit 100;