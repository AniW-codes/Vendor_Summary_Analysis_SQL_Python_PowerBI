import pandas as pd
import psycopg2
from ingestion_db import ingest_db

def vendor_sales_summary(engine):
    """This function will collaborate all necessary data together and add columns needed 
    for analysis"""
    from sqlalchemy import create_engine, text

# Create SQLAlchemy engine
    engine = create_engine("postgresql+psycopg2://postgres:Mosalah11!@localhost:5432/postgres")

    # SQL query to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS vendor_sales_summary (
        "VendorNumber"                INT,
        "VendorName"                  VARCHAR(255),
        "Brand"                       INT,
        "Description"                 VARCHAR(255),
        "PurchasePrice"               FLOAT,
        "actual_price"                FLOAT,
        "Volume"                      FLOAT,
        "total_purchase_qty"          FLOAT,
        "total_purchase_dollars"      FLOAT,
        "total_sales_dollars"         FLOAT,
        "total_sales_price"           FLOAT,
        "total_sales_qty"             FLOAT,
        "excisetax"                   FLOAT,
        "frieghtcost"                 FLOAT,
        "grossprofit"                 FLOAT,
        "profitmargin"                FLOAT,
        "stockturnover"               FLOAT,
        "sales_to_purchase_ratio"     FLOAT,
        PRIMARY KEY ("VendorNumber", "Brand")
    );
    """

    # Execute using SQLAlchemy engine
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
            print("✅ Table created successfully using SQLAlchemy.")
    except Exception as e:
        print(f"❌ Error during table creation: {e}")

    return vendor_sales_summary

def clean_data(df_final):
    """This function will clean the data"""

    #Fills all null numeric values with 0
    df_final.fillna(0,inplace=True)

    #Removes extra spaces in VendorName column
    df_final['VendorName'] = df_final['VendorName'].str.strip()

    #Creating new columns for better analysis
    df_final['grossprofit'] = df_final['total_sales_dollars']- df_final['total_purchase_dollars']
    df_final['profitmargin'] = (df_final['grossprofit']/df_final['total_sales_dollars'])*100
    df_final['stockturnover'] = (df_final['total_sales_qty']/df_final['total_purchase_qty'])
    df_final['sales_to_purchase_ratio'] = (df_final['total_sales_dollars']/df_final['total_purchase_dollars'])

    return df_final

if __name__ == "__main__":
    #Creating connection
    engine = create_engine("postgresql+psycopg2://postgres:Mosalah11!@localhost:5432/postgres")

    clean_data(df_final)

    

