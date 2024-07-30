import os
import json
from jinja2 import FileSystemLoader, Environment
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import udf, struct, lit


class FHIRTransformer(object):
    def __init__(self, templates_dir: str = None, template_name: str = None) -> None:
        if templates_dir is None:
            templates_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "templates"
            )
        file_loader = FileSystemLoader(templates_dir)
        self._env = Environment(loader=file_loader, autoescape=True)
        self._template_name = template_name
        self._max_filter = 5

    def load_template(self, template_name: str) -> None:
        self._template_name = template_name

    def render_resource(self, resource_type: str, data: dict) -> str:
        """
        Renders FHIR resource based on resource type and data
        """
        template = self._env.get_template(self._template_name)
        rendered_data = template.render(resource_type=resource_type, data=data)
        filtered_data = json.dumps(self._filter_fields(json.loads(rendered_data)))
        # clean up none, empty fields from rendered data
        filter_count = 1
        while rendered_data != filtered_data:
            rendered_data = filtered_data
            filtered_data = json.dumps(self._filter_fields(json.loads(filtered_data)))
            filter_count += 1
            if filter_count == self._max_filter:
                break
        return filtered_data

    # def render_resource_df(
    #     self, column: str, resource_type: str, df: DataFrame
    # ) -> DataFrame:
    #     render_udf = udf(
    #         lambda row, resource_type: self.render_resource(resource_type, row.asDict())
    #     )
    #     return df.withColumn(
    #         column, render_udf(struct([df[x] for x in df.columns]), lit(resource_type))
    #     )

    def _filter_fields(self, value):
        """
        Recursively remove all Empty and None values from dictionaries and lists, and returns
        the result as a new dictionary or list.
        """
        if isinstance(value, list):
            return [
                self._filter_fields(val)
                for val in value
                if ( isinstance(val, bool) or val)
                and (
                    not isinstance(val, str) or (isinstance(val, str) and val != "None")
                )
            ]
        elif isinstance(value, dict):
            return {
                key: self._filter_fields(val)
                for key, val in value.items()
                if ( isinstance(val, bool) or val)
                and (
                    not isinstance(val, str) or (isinstance(val, str) and val != "None")
                )
            }
        else:
            return value
