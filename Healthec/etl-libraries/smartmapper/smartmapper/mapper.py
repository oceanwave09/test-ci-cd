import os
import importlib.util
from inspect import getmembers, isfunction
from collections import namedtuple
from pyspark.sql.functions import trim, when, lit, udf
from pyspark.sql import DataFrame

from smartmapper.schema import Schema
import smartmapper.utils as utils


FieldOpts = namedtuple("FieldOpts", "start len pos src fillna type rule")


class SmartMapper:
    """ SmartMapper is used to transform the incoming data into the structure defined
    in the schema.
    """

    def __init__(self):
        self._schema = None
        self._rules = {}
        self._call_udf = None
        # set udf
        self._set_call_udf()

    @property
    def rules(self):
        """ getter for attribute 'rules' """
        return self._rules

    @property
    def schema(self):
        """ getter for attribute 'schema' """
        return self._schema

    @classmethod
    def new_mapper(cls):
        """ creates new instance of the class """
        return cls()

    def set_schema(self, filepath: str):
        """ reads the file and sets schema for the mapper """
        self._schema = Schema.load_from_file(filepath)
        print(self._schema)

    def load_rules(self, filepaths: str = ""):
        """ loads common and user rules into dict """
        # load common rules from utils
        self._rules = dict(getmembers(utils, isfunction))
        # check filepaths and load user rules
        if not filepaths:
            return
        for filepath in filepaths.split(";"):
            if not os.path.exists(filepath):
                continue
            # load the given python file as module
            spec = importlib.util.spec_from_file_location(
                "user_rules", filepath)
            user_rules = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(user_rules)
            # append user rules into existing rules dict
            self._rules = {**self._rules,
                           **dict(getmembers(user_rules, isfunction))}

    def map_df(self, df: DataFrame):
        """ parse the dataframe row with the given schema """
        destination_fields = []
        for field, field_opts_str in self._schema.field_mappings.items():
            field_opts = self._parse_field_opts(field_opts_str)
            df = self._parse_entry(df, field, field_opts)
            destination_fields.append(field)
        for rule in self._schema.validation_rules:
            df = df.transform(self._rules[rule])
        destination_fields.extend(self._schema.include_fields)
        destination_fields = [exclude_field for exclude_field in destination_fields
                              if exclude_field not in self._schema.exclude_fields]
        return df.select(*destination_fields)

    def _set_call_udf(self):
        """ set up a common udf to call rules by name with arguments """
        self._call_udf = udf(lambda df, rule: self._apply_rule(df, rule))

    def _apply_rule(self, df, rule):
        """ calls the function """
        return self._rules[rule](df)

    def _parse_field_opts(self, field_opts_str: str) -> FieldOpts:
        field_opts_dict = {"start": 0, "len": 0, "pos": 0, "src": "",
                           "fillna": "", "type": "string", "rule": ""}
        for field_opt in field_opts_str.split(";"):
            opt = field_opt.split("=")
            if opt[0] in ["start", "len", "pos"]:
                field_opts_dict[opt[0]] = int(opt[1])
            elif opt[0] in ["src", "fillna", "type", "rule"]:
                field_opts_dict[opt[0]] = opt[1]
        return FieldOpts(**field_opts_dict)

    def _parse_entry(self, df: DataFrame, field: str, field_opts: FieldOpts):
        if self.schema.is_delimited_file:
            if field_opts.src != "":
                column = trim(df[field_opts.src])
            elif field_opts.pos != "":
                column = trim(df[field_opts.pos - 1])
            else:
                column = lit(field_opts.fillna)
        else:
            if field_opts.start != "":
                column = trim(df.value.substr(
                    field_opts.start, field_opts.len))
            else:
                column = lit(field_opts.fillna)
        df = df.withColumn(field, when(column == "", lit(field_opts.fillna).cast(field_opts.type))
                           .otherwise(column.cast(field_opts.type)))
        if field_opts.rule:
            df = df.withColumn(field, self._call_udf(
                df[field], lit(field_opts.rule)))
        return df
