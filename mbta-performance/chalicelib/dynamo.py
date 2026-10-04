import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


def dynamo_update_items(items: list[dict], table_name: str, key_fields: tuple[str, ...] = ("route", "date")) -> None:
    """Upsert items into a DynamoDB table, SETting only the attributes each item carries.

    Unlike put_item, this leaves any other attributes already on the row untouched, so
    several jobs can each own their own fields on a shared row and run in any order. DynamoDB
    has no batch form of UpdateItem, so this is one request per item.
    """
    if not items:
        return
    table = dynamodb.Table(table_name)
    for item in items:
        fields = [field for field in item if field not in key_fields]
        if not fields:
            continue
        # Every name goes through a placeholder, since some (e.g. "count") are reserved words.
        table.update_item(
            Key={field: item[field] for field in key_fields},
            UpdateExpression="SET " + ", ".join(f"#f{i} = :v{i}" for i in range(len(fields))),
            ExpressionAttributeNames={f"#f{i}": field for i, field in enumerate(fields)},
            ExpressionAttributeValues={f":v{i}": item[field] for i, field in enumerate(fields)},
        )


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
