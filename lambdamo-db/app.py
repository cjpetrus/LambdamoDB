from chalice import Chalice
import logging
import json
import cruddy
import botocore
import boto3

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

app = Chalice(app_name='lambdamo-db')

"""The method to serialize the Python data types.

:param value: A python value to be serialized to DynamoDB. Here are
    the various conversions:

    Python                                  DynamoDB
    ------                                  --------
    None                                    {'NULL': True}
    True/False                              {'BOOL': True/False}
    int/Decimal                             {'N': str(value)}
    string                                  {'S': string}
    Binary/bytearray/bytes (py3 only)       {'B': bytes}
    set([int/Decimal])                      {'NS': [str(value)]}
    set([string])                           {'SS': [string])
    set([Binary/bytearray/bytes])           {'BS': [bytes]}
    list                                    {'L': list}
    dict                                    {'M': dict}

    For types that involve numbers, it is recommended that ``Decimal``
    objects are used to be able to round-trip the Python type.
    For types that involve binary, it is recommended that ``Binary``
    objects are used to be able to round-trip the Python type.

:rtype: dict
:returns: A dictionary that represents a dynamoDB data type. These
    dictionaries can be directly passed to botocore methods.


    """


@app.route('/')
def index():
    dynamo = boto3.resource('dynamodb')
    operations = {
        'create': lambda x: dynamo.put_item(Item=x),
        'read': lambda x: dynamo.get_item(Key=x),
        'update': lambda x: dynamo.update_item(x), # cannot execute
        'delete': lambda x: dynamo.delete_item(Key=x),
        'list': lambda x: dynamo.scan(Item=x), # cannot execute
        'echo': lambda x: x,
        'ping': lambda x: 'pong'
    }




def create_table(table_name, hash_key, hash_key_type='S', range_key='', range_key_type='',
                 read_capacity=5, write_capacity=5):
    """Parameters:
    table_name (basestring) - Required name of the table
    hash_key (int|long|float|str|unicode|Binary) – The HashKey of the requested item. The type of the value must match the type defined in the schema for the table.
    range_key (int|long|float|str|unicode|Binary) – The optional RangeKey of the requested item. The type of the value must match the type defined in the schema for the table.
    """

    client = boto3.client('dynamodb')
    try:
        response = client.describe_table(TableName=table_name)
    except botocore.exceptions.ClientError as e:
        print("DynamoDB table '" + table_name +
              "' does not appear to exist, creating...")
        dynamodb = boto3.resource('dynamodb')
        key_schema = [{'AttributeName': hash_key,
                       'KeyType': 'HASH'}]
        attr_schema = [{
            'AttributeName': hash_key,
            'AttributeType': hash_key_type
        }]
        if range_key:
            key_schema.append({'AttributeName': range_key,
                               'KeyType': 'RANGE'})
            attr_schema.append({
            'AttributeName': range_key,
            'AttributeType': range_key_type
        })
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_schema,
            ProvisionedThroughput={'ReadCapacityUnits': read_capacity,
                                   'WriteCapacityUnits': write_capacity}
        )
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print("DynamoDB table '" + table_name + "' created.")






# def create_table_simple(table_name, hash_key, hash_key_type='S', range_key='', range_key_type='S', attr_defs=None,
#                         read_capacity=5, write_capacity=5):
#
#
#     table_schema = conn.create_schema(
#         tmp_schema
#     )
#
#     conn = boto.connect_dynamodb(aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
#                                  aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
#
#     table_schema = conn.create_schema(
#         hash_key_name='item_id',
#         hash_key_proto_value='S',
#         range_key_name='date_found',
#         range_key_proto_value='S'
#     )
#
#     table = conn.create_table(
#         name='counter',
#         schema=table_schema,
#         read_units=10,
#         write_units=10
#     )



# def handler(event, context):
#     LOG.info(event)
#     response = crud.handler(**event)
#     return response.flatten()

# The view function above will return {"hello": "world"}
# whenver you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users/', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.json_body
#     # Suppose we had some 'db' object that we used to
#     # read/write from our database.
#     # user_id = db.create_user(user_as_json)
#     return {'user_id': user_id}
#
# See the README documentation for more examples.
#
