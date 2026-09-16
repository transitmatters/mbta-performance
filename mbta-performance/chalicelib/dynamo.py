import boto3

dynamodb = boto3.resource("dynamodb")


def dynamo_batch_write(items: list[dict], table_name: str) -> None:
    """Write items to a DynamoDB table. Oversize batches are split automatically."""
    if not items:
        return
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def create_table_if_not_exists(table_name: str, hash_key: str, range_key: str) -> None:
    """Create a string-keyed, on-demand table if it doesn't already exist.

    Matches how the existing DeliveredTripMetrics family is provisioned: PAY_PER_REQUEST
    billing, string hash and range keys, no secondary indexes. Idempotent, so it's safe to
    call at the top of a script rather than as a one-off manual setup step.
    """
    existing_tables = dynamodb.meta.client.list_tables()["TableNames"]
    if table_name in existing_tables:
        return
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": hash_key, "KeyType": "HASH"},
            {"AttributeName": range_key, "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": hash_key, "AttributeType": "S"},
            {"AttributeName": range_key, "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
