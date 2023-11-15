#Importing libraries
import streamlit as st
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from DQEngine.dqrulechks import *
from common_functions.connectionparam import connection_parameters


def dqEngine():
    spark = SparkSession.builder.appName("data-quality-engine").master('local[5]').getOrCreate()

    def applyDQ(spark):
        try:
            if source_file_format.lower() == "csv" and dq_file_format.lower() == 'csv':
                df = spark.read.csv(source_file, sep=',',inferSchema=True, header=True)
            elif source_file_format.lower() == "parquet" and dq_file_format.lower() == 'parquet':
                df = spark.read.parquet(source_file)
            elif source_file_format.lower() == "json" and dq_file_format.lower() == 'json':
                df = spark.read.json(source_file)
            dq_mapping_table = spark.read.csv(dq_mapping_file, sep=',',inferSchema=True, header=True)

            # new_dq_mapping_table = dq_mapping_table.filter(col('table_name')==tgt_table)

            df_new = df
            for row in range(dq_mapping_table.count()):
                col_name = dq_mapping_table.collect()[row]['column_name']
                chk_name = dq_mapping_table.collect()[row]['dq_rule']
                if chk_name.lower() == 'int_chk':
                    chk = int_chk(col_name)
                elif chk_name.lower() == 'decimal_chk':
                    chk = decimal_chk(col_name)
                elif chk_name.lower() == 'email_chk':
                    chk = email_chk(col_name)
                elif chk_name.lower() == 'numeric_chk':
                    chk = numeric_chk(col_name)
                elif chk_name.lower() == 'datetime_chk':
                    chk = datetime_chk(col_name)
                elif chk_name.lower() == 'boolean_chk': 
                    chk = boolean_chk(col_name)
                elif chk_name.lower() == 'not_null_chk':
                    chk = not_null_chk(col_name)
                
                df_new = df_new.select('*',chk.alias(f'{col_name}_{chk_name}'))
                df_new = df_new.filter((col(f'{col_name}_{chk_name}')==True)).select('*')
        
            df_ = df_new.select(df.columns)
            row_count = df_.count()
            with st.container():
                st.write(f'Total {row_count} number of records are valid records.')
                st.download_button(label="Download Validated Data",data=df_,file_name='Validated_data.csv')

        except Exception as e:
            st.error("Error : {e}".format(e))
    with st.form('dq-engine'):
        left_col, right_col = st.columns(2)
        with left_col:
            source_file_format = st.selectbox('Select Source File Format',('Select File Format...','Csv', 'Parquet', 'Json'))
            source_file = st.file_uploader('Choose a file',key=1)
        with right_col:
            dq_file_format = st.selectbox('Select DQ Mapping File Format',('Select File Format...','Csv', 'Parquet', 'Json'))
            dq_mapping_file = st.file_uploader('Choose a file',key=2)
        applied = st.form_submit_button("Apply Data Quality On Data")
        if applied:
            if source_file_format is not None and dq_file_format is not None and applied:
                applyDQ(spark)


