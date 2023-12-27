//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.dt_demo;

--Second table is PROD_STOCK_INV and insert 100 products inventory into it using this new Python UDTF
create or replace function gen_prod_inv(num_records number)
returns table (pid number(10), pname varchar(100), stock number(10,2), stockdate date)
language python
runtime_version=3.10
handler='ProdTab'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100 # Starting customer ID                 
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (3 months from now)
            min_date = current_date - timedelta(days=90)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date,current_date)

            product_id += 1
            yield (pid,pname,stock,stockdate)

$$;

//Validate the function
show functions like 'gen_prod_inv';

//Create PROD_STOCK_INV table and insert 100 products inventory into it using this new Python UDTF.
create or replace table prod_stock_inv as select * from table(gen_prod_inv(100)) order by 1;

//Validate table records
select * from prod_stock_inv limit 100;