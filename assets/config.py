import os
import json
import pandas as pd
import numpy as np
import configparser
from snowflake.snowpark import Session

# Read snowflake credentials securely
config = configparser.ConfigParser()
config.read('assets/credentials.cfg')

connection_parameters = dict(
    account   =  config['SNOWPARKAWS']['SNOWFLAKE_ACCOUNT'],
    user      =  config['SNOWPARKAWS']['SNOWFLAKE_USER'],
    password  =  config['SNOWPARKAWS']['SNOWFLAKE_PASSWORD'],
    role      =  config['SNOWPARKAWS']['SNOWFLAKE_ROLE'],  # optional
    warehouse =  config['SNOWPARKAWS']['SNOWFLAKE_WAREHOUSE'],  # optional
    database  =  config['SNOWPARKAWS']['SNOWFLAKE_DATABASE'],  # optional
    schema    =  config['SNOWPARKAWS']['SNOWFLAKE_SCHEMA'],  # optional
)

# Pass this dictionary to the Session.builder.configs method to return a builder object that has these connection parameters.
# Call the create method of the builder to establish the session.
def connection_builder():
    session = Session.builder.configs(connection_parameters).create()
    return session