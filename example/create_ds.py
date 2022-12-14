import great_expectations as gx
from yaml import load, Loader

if __name__ == '__main__':
    context = gx.get_context()
    datasource_name = "super_store_sales"
    datasource_config = f"""name: {datasource_name}
class_name: Datasource
execution_engine:
  class_name: SparkDFExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    class_name: InferredAssetS3DataConnector
    bucket: big-data-tools-pasha-demo
    default_regex:
      pattern: (.*\\.csv)
      prefix: Demo/
      group_names:
        - sales
    batch_spec_passthrough:
        reader_method: csv
        reader_options:
            header: yes
            inferSchema: yes
  default_runtime_data_connector_name:
    class_name: RuntimeDataConnector
    assets:
      my_runtime_asset_name:
        batch_identifiers:
          - runtime_batch_identifier_name
    """
    context.test_yaml_config(datasource_config)
    context.add_datasource(**load(datasource_config, Loader=Loader))
