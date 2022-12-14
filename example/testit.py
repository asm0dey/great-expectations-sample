import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError

context = gx.data_context.DataContext()

batch_request = {
    "datasource_name": "super_store_sales",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": "DEFAULT_ASSET_NAME",
    "limit": 100000,
}

expectation_suite_name = "sales"
try:
    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    print(
        f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
    )
except DataContextError:
    suite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name,
)

validation_result = validator.expect_column_distinct_values_to_equal_set(
    column='ship_mode',
    value_set=["First Class",
               "Same Day",
               "Second Class",
               ],
    result_format="COMPLETE",
)
print(validation_result)
validator.save_expectation_suite(discard_failed_expectations=True)
