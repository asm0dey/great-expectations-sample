import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.exceptions import DataContextError

context = gx.data_context.DataContext()

# Note that if you modify this batch request, you may save the new version as a .json file
#  to pass in later via the --batch-request option
batch_request = {
    "datasource_name": "super_store_sales",
    "data_connector_name": "default_inferred_data_connector_name",
    "data_asset_name": "DEFAULT_ASSET_NAME",
    "limit": 100000,
}

# Feel free to change the name of your suite here. Renaming this will not remove the other one.
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
# column_names = [f'"{column_name}"' for column_name in validator.columns()]
# print(f"Columns: {', '.join(column_names)}.")
# validator.head(n_rows=5, fetch_all=False)
validator.save_expectation_suite(discard_failed_expectations=True)
