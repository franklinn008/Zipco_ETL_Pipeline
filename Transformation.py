import pandas as pd
from Extraction import run_extraction

def run_transformation():
    #reading in the dataset
    data = pd.read_csv('zipco_transaction.csv')
    
    #Dropping duplicates
    data.drop_duplicates(inplace=True)

    #Replacing missing/null values, filling numerical values with the mean/median
    num_columns = data.select_dtypes(include= ['Float64','int64']).columns
    for col in num_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    #Replacing missing/null values, filling categorical/object values with unknown
    cat_columns = data.select_dtypes(include= ['object']).columns
    for col in cat_columns:
        data.fillna({col: 'unknown'}, inplace=True)

    #CHANGING DATE DATATYPE
    data['Date'] = pd.to_datetime(data['Date'])

    #Creating Tables
    #creating product table
    products = data[['ProductName']].copy().drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    #creating customers table
    customers = data[['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail']].copy().drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    #creating staff table
    staff  = data[['Staff_Name','Staff_Email']].copy().drop_duplicates().reset_index(drop=True)
    staff .index.name = 'StaffID'
    staff  = staff .reset_index()

    #creating Transactions fact table
    transactions = data.merge(customers,on=['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail'], how= 'left')\
                        .merge (products, on=['ProductName'],how= 'left')\
                        .merge( staff, on=['Staff_Name','Staff_Email'],how='left') \
                        [['ProductID','CustomerID','StaffID','Date','Quantity', 'UnitPrice', 'StoreLocation','PaymentType', 'PromotionApplied','Weather',\
                        'Temperature','StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min','OrderType','DayOfWeek','TotalSales']]
    transactions.index.name ='TransactionID'
    transactions = transactions.reset_index()

    print('Data cleaning, Transforming and Tables completed')

