import snowflake.snowpark

def refresh_login_history(session: snowflake.snowpark.Session, status:bool) -> str:
    if (status):
        login_df = session.table('SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY')
        login_df.write.save_as_table(table_name="loginhistory", mode="overwrite")
        return 'refresh ran successfully'
    return 'table not refreshed'