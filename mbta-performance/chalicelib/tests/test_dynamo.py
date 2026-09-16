import unittest
from unittest import mock

from .. import dynamo as dynamo_module


class TestDynamoBatchWrite(unittest.TestCase):
    def setUp(self):
        self.mock_dynamodb = mock.MagicMock()
        self.mock_table = mock.MagicMock()
        self.mock_dynamodb.Table.return_value = self.mock_table
        self.mock_batch = mock.MagicMock()
        self.mock_table.batch_writer.return_value.__enter__.return_value = self.mock_batch

    def test_writes_each_item_through_the_batch_writer(self):
        items = [{"route": "1", "date": "2026-09-03"}, {"route": "2", "date": "2026-09-03"}]

        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_batch_write(items, "SomeTable")

        self.mock_dynamodb.Table.assert_called_once_with("SomeTable")
        self.assertEqual(self.mock_batch.put_item.call_count, 2)
        self.mock_batch.put_item.assert_any_call(Item=items[0])
        self.mock_batch.put_item.assert_any_call(Item=items[1])

    def test_empty_items_does_not_touch_dynamo(self):
        with mock.patch("chalicelib.dynamo.dynamodb", self.mock_dynamodb):
            dynamo_module.dynamo_batch_write([], "SomeTable")

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
