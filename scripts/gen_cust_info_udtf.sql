//set worksheet context
use role sysadmin;
use warehouse compute_wh;
use schema demo_db.dt_demo;

--First table is CUST_INFO and insert 1000 customers into it using this new Python UDTF.
create or replace function gen_cust_info(num_records number)
returns table (custid number(10), cname varchar(100), spendlimit number(10,2))
language python
runtime_version=3.10
handler='CustTab'
packages = ('Faker')
as $$
from faker import Faker
import random
fake = Faker()
# Generate a list of customers  

class CustTab:
    # Generate multiple customer records
    def process(self, num_records):
        customer_id = 1000 # Starting customer ID                 
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000),2)
            customer_id += 1
            yield (custid,cname,spendlimit)

$$;

//Validate the function
show functions like 'gen_cust_info';

//Create CUST_INFO table and insert 1000 customers into it using this new Python UDTF: gen_cust_info.
create or replace table cust_info as select * from table(gen_cust_info(1000)) order by 1;

//Validate table records
select * from cust_info limit 100;