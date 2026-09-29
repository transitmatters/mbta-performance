import unittest
from unittest import mock

from .. import dynamo as dynamo_module


class TestDynamoUpdateItems(unittest.TestCase):
    def setUp(self):
        self.mock_dynamodb = mock.MagicMock()
        self.mock_table = mock.MagicMock()
        self.mock_dynamodb.Table.return_value = self.mock_table

    def test_sets_only_the_non_key_fields_of_each_item(self):
        items = [
            {"route": "1", "date": "2026-09-03", "count": 10, "total_time": 600},
            {"route": "2", "date": "2026-09-03", "count": 4, "total_time": 240},
        ]

        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_update_items(items, "SomeTable")

        self.mock_dynamodb.Table.assert_called_once_with("SomeTable")
        self.mock_table.put_item.assert_not_called()
        self.assertEqual(self.mock_table.update_item.call_count, 2)
        self.mock_table.update_item.assert_any_call(
            Key={"route": "1", "date": "2026-09-03"},
            UpdateExpression="SET #f0 = :v0, #f1 = :v1",
            ExpressionAttributeNames={"#f0": "count", "#f1": "total_time"},
            ExpressionAttributeValues={":v0": 10, ":v1": 600},
        )

    def test_custom_key_fields_are_kept_out_of_the_update(self):
        items = [{"stop": "place-sstat", "day": "2026-09-03", "value": 1}]

        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_update_items(items, "SomeTable", key_fields=("stop", "day"))

        self.mock_table.update_item.assert_called_once_with(
            Key={"stop": "place-sstat", "day": "2026-09-03"},
            UpdateExpression="SET #f0 = :v0",
            ExpressionAttributeNames={"#f0": "value"},
            ExpressionAttributeValues={":v0": 1},
        )

    def test_an_item_with_only_key_fields_is_skipped(self):
        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_update_items([{"route": "1", "date": "2026-09-03"}], "SomeTable")

        self.mock_table.update_item.assert_not_called()

    def test_empty_items_does_not_touch_dynamo(self):
        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_update_items([], "SomeTable")

        self.mock_dynamodb.Table.assert_not_called()


class TestCreateTableIfNotExists(unittest.TestCase):
    def setUp(self):
        self.mock_dynamodb = mock.MagicMock()

    def test_does_nothing_when_the_table_already_exists(self):
        self.mock_dynamodb.meta.client.list_tables.return_value = {"TableNames": ["SomeTable", "OtherTable"]}

        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.create_table_if_not_exists("SomeTable", hash_key="route", range_key="date")

        self.mock_dynamodb.create_table.assert_not_called()

    def test_creates_an_on_demand_table_with_string_keys_when_missing(self):
        self.mock_dynamodb.meta.client.list_tables.return_value = {"TableNames": ["OtherTable"]}
        mock_table = mock.MagicMock()
        self.mock_dynamodb.create_table.return_value = mock_table

        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.create_table_if_not_exists("SomeTable", hash_key="route", range_key="date")

        self.mock_dynamodb.create_table.assert_called_once_with(
            TableName="SomeTable",
            KeySchema=[
                {"AttributeName": "route", "KeyType": "HASH"},
                {"AttributeName": "date", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "route", "AttributeType": "S"},
                {"AttributeName": "date", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        mock_table.wait_until_exists.assert_called_once()
