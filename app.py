import mysql.connector
import streamlit as st
import pandas as pd

mydb = mysql.connector.connect(host='localhost',user='root', password='pass123',database='streaming_data')
cursor = mydb.cursor()

# Function to fetch database tables and their columns
def fetch_tables_and_columns():
    try:
        cursor.execute("SHOW TABLES;")
        tables = [table[0] for table in cursor.fetchall()]
        table_info = {}
        for table in tables:
            cursor.execute(f"DESCRIBE {table};")
            columns = [column[0] for column in cursor.fetchall()]
            table_info[table] = columns
        return table_info
    except Exception as e:
        st.error(f"Error fetching tables and columns: {e}")
        return None

# Function to execute SQL query
def execute_query(query):
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return results, columns
    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
        return None, None


def main():
    st.title("MYSql Query Tool")
    table_info = fetch_tables_and_columns()
    if table_info:
        st.sidebar.subheader('Database: streaming_data')
        for table, columns in table_info.items():
            st.sidebar.write(f"**{table}**")
            st.sidebar.write(columns)
    query = st.text_area('Enter your SQL query:', 'SELECT * FROM movies;')

    # Execute query on button click
    if st.button('Run Query'):
        results, columns = execute_query(query)
        
        # Display results as table
        if results and columns:
            df = pd.DataFrame(results, columns=columns)
            st.write(df)
        else:
            st.warning('No results to display.')


if __name__== "__main__":
    main()