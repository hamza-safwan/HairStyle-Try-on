from flask import Flask, jsonify, request, render_template
import boto3
from boto3.dynamodb.conditions import Key
import csv
import os


app = Flask(__name__)

# Initialize DynamoDB local instance
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000',  # Use DynamoDB Local
    region_name='us-west-2',               # You can use any region
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy'
)

# Function to create a table with GSI and LSI
def create_products_table():
    try:
        table = dynamodb.create_table(
            TableName='Products',
            KeySchema=[
                {'AttributeName': 'product_id', 'KeyType': 'HASH'},  # Partition Key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'product_id', 'AttributeType': 'S'},  # Primary key
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10,
            }
        )
        print(f"Creating table: {table.name}...")
        table.meta.client.get_waiter('table_exists').wait(TableName='Products')
        print(f"Table {table.name} created successfully!")
    except Exception as e:
        print(f"Error: {e}")

def create_customers_table():
    try:
        table = dynamodb.create_table(
            TableName='Customers',
            KeySchema=[
                {'AttributeName': 'customer_id', 'KeyType': 'HASH'},  # Partition Key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'customer_id', 'AttributeType': 'S'},  # Primary key
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10,
            }
        )
        print(f"Creating table: {table.name}...")
        table.meta.client.get_waiter('table_exists').wait(TableName='Customers')
        print(f"Table {table.name} created successfully!")
    except Exception as e:
        print(f"Error: {e}")

def create_orders_table():
    try:
        existing_tables = dynamodb.tables.all()
        if 'Orders' not in [table.name for table in existing_tables]:
            table = dynamodb.create_table(
                TableName='Orders',
                KeySchema=[
                    {'AttributeName': 'order_id', 'KeyType': 'HASH'},  # Partition Key
                    {'AttributeName': 'order_date', 'KeyType': 'RANGE'},  # Sort Key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'order_id', 'AttributeType': 'S'},
                    {'AttributeName': 'product_id', 'AttributeType': 'S'},
                    {'AttributeName': 'order_date', 'AttributeType': 'S'},
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10,
                },
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'ProductIndex',
                        'KeySchema': [
                            {'AttributeName': 'product_id', 'KeyType': 'HASH'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 10,
                            'WriteCapacityUnits': 10,
                        }
                    }
                ],
                LocalSecondaryIndexes=[
                    {
                        'IndexName': 'OrderDateIndex',
                        'KeySchema': [
                            {'AttributeName': 'order_id', 'KeyType': 'HASH'},
                            {'AttributeName': 'order_date', 'KeyType': 'RANGE'},
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            )
            print(f"Creating table: {table.name}...")
            table.meta.client.get_waiter('table_exists').wait(TableName='Orders')
            print(f"Table {table.name} created successfully!")
        else:
            print("Table 'Orders' already exists!")
    except Exception as e:
        print(f"Error: {e}")


# Call the function to create the Orders table
create_orders_table()
create_products_table()
create_customers_table()

# Serve the index.html file
@app.route('/')
def home():
    return render_template('index.html')

# Route to add order to DynamoDB
@app.route('/add_order', methods=['POST'])
def add_order():
    table = dynamodb.Table('Orders')
    
    order_id = request.json['order_id']
    product_id = request.json['product_id']
    customer_id = request.json['customer_id']
    order_date = request.json['order_date']
    quantity = request.json['quantity']
    status = request.json['status']
    total_price = request.json['total_price']

    table.put_item(
        Item={
            'order_id': order_id,
            'product_id': product_id,
            'customer_id': customer_id,
            'order_date': order_date,
            'quantity': quantity,
            'status': status,
            'total_price': total_price
        }
    )

    return jsonify({"message": "Order added successfully!"})

# lab tasks queries
@app.route('/query_by_product/<product_id>', methods=['GET'])
def query_by_product(product_id):
    table = dynamodb.Table('Orders')
    
    response = table.query(
        IndexName='ProductIndex',
        KeyConditionExpression=Key('product_id').eq(product_id)
    )

    return jsonify(response['Items'])


@app.route('/query_by_order_date/<order_id>/<order_date>', methods=['GET'])
def query_by_order_date(order_id, order_date):
    table = dynamodb.Table('Orders')
    
    response = table.query(
        IndexName='OrderDateIndex',
        KeyConditionExpression=Key('order_id').eq(order_id) & Key('order_date').eq(order_date)
    )

    return jsonify(response['Items'])

# data import
data_folder = os.path.join(os.path.dirname(__file__), 'data')

def import_data(table_name, file_name):
    table = dynamodb.Table(table_name)
    file_path = os.path.join(data_folder, file_name)
    
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                table.put_item(Item=row)
        print(f"Data imported to {table_name} table successfully.")
    except Exception as e:
        print(f"Error while importing data to {table_name}: {e}")


@app.route('/import')
def import_page():
    return render_template('import.html')


@app.route('/import_products', methods=['GET'])
def import_products():
    import_data('Products', 'df_Products.csv')
    return render_template('import.html', message="Products data imported successfully!")


@app.route('/import_customers', methods=['GET'])
def import_customers():
    import_data('Customers', 'df_Customers.csv')
    return render_template('import.html', message="Customers data imported successfully!")


@app.route('/import_orders', methods=['GET'])
def import_orders():
    import_data('Orders', 'df_Orders.csv')
    return render_template('import.html', message="Orders data imported successfully!")

# lab tasks queries
@app.route('/query_orders_by_product_date', methods=['GET'])
def query_orders_by_product_date():
    product_id = request.args.get('product_id')
    table = dynamodb.Table('Orders')

    response = table.query(
        IndexName='ProductIndex',
        KeyConditionExpression=Key('product_id').eq(product_id) & 
                               Key('order_date').between('2023-01-01', '2023-12-31')
    )
    
    return jsonify(response['Items'])


@app.route('/sort_products_by_price', methods=['GET'])
def sort_products_by_price():
    table = dynamodb.Table('Products')

    response = table.scan()
    
    sorted_items = sorted(response['Items'], key=lambda x: float(x['price']))
    
    return jsonify(sorted_items)

@app.route('/filter_orders_by_status_customer', methods=['GET'])
def filter_orders_by_status_customer():
    status = request.args.get('status')
    customer_id = request.args.get('customer_id')
    table = dynamodb.Table('Orders')

    response = table.scan(
        FilterExpression=Key('status').eq(status) & Key('customer_id').eq(customer_id)
    )
    
    return jsonify(response['Items'])

@app.route('/query_orders_for_customer', methods=['GET'])
def query_orders_for_customer():
    customer_id = request.args.get('customer_id')
    table = dynamodb.Table('Orders')
    
    response = table.query(
        KeyConditionExpression=Key('customer_id').eq(customer_id),
        ScanIndexForward=True  
    )
    
    return jsonify(response['Items'])

if __name__ == '__main__':
    app.run(debug=True)

