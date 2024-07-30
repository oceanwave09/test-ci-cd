from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from deidentifier.utils.constants import (CSV_FILE, JSON_FILE, NUMERIC_REPLACE_VALUES,
                                          ALPHABETIC_REPLACE_VALUES)


class DeIdentifier:
    def __init__(self, file_context, file_df, file_format=CSV_FILE):
        self.file_context = file_context
        self.file_df = file_df
        # temporary stub, can be used in future for different file formats
        self.file_format = file_format

    # figure 1
    def deid_ssn_tax_id(self, column_name='ssn'):
        mapping_expr = F.create_map([F.lit(x) for i in NUMERIC_REPLACE_VALUES.items() for x in i])

        self.file_df = (
            self.file_df
            .withColumn(
                f'{column_name}_deid',
                F.concat(
                    mapping_expr[F.substring(column_name, 1, 1)],
                    F.substring(column_name, 2, 2),
                    mapping_expr[F.substring(column_name, 4, 1)],
                    F.substring(column_name, 5, 2),
                    mapping_expr[F.substring(column_name, 7, 1)],
                    F.substring(column_name, 8, 2)
                )
            )
        )

    # figure 2
    def deid_health_plan_identifiers(self, column_name='ssn'):
        mapping_expr = F.create_map([F.lit(x) for i in ALPHABETIC_REPLACE_VALUES.items() for x in i])

        self.file_df = (
            self.file_df
            .withColumn(
                f'{column_name}_deid',
                F.concat(
                    F.substring(column_name, 1, 1),
                    mapping_expr[F.substring(column_name, 2, 1)],
                    mapping_expr[F.substring(column_name, 3, 1)],
                    F.substring(column_name, 4, 1),
                    mapping_expr[F.substring(column_name, 5, 1)],
                    mapping_expr[F.substring(column_name, 6, 1)],
                    F.substring(column_name, 7, 1),
                    mapping_expr[F.substring(column_name, 8, 1)],
                    mapping_expr[F.substring(column_name, 9, 1)],
                )
            )
        )

    # figure 3
    def deid_first_name(self, column_name='first_name', ssn_tax_column_name='ssn'):
        deid_col_name = f'{ssn_tax_column_name}_deid'
        self.file_df = self.file_df.withColumn(
            column_name,
            F.format_string('%s%s', F.lit('First'), deid_col_name)
        )

    def deid_last_name(self, column_name='last_name', ssn_tax_column_name='ssn'):
        deid_col_name = f'{ssn_tax_column_name}_deid'
        self.file_df = self.file_df.withColumn(
            column_name,
            F.format_string('%s%s', F.lit('Last'), deid_col_name)
        )

    def deid_middle_name(self, column_name='middle_name'):
        self.file_df = self.file_df.withColumn(column_name, F.lit(None).cast(StringType()))

    def deid_str_addr_1(self, column_name='street_address_1'):
        self.file_df = self.file_df.withColumn(column_name, F.lit("343 Thornall Street"))

    def deid_str_addr_2(self, column_name='street_address_2'):
        self.file_df = self.file_df.withColumn(column_name, F.lit('Suite #630'))

    def deid_birth_date(self, column_name='birth_date'):
        self.file_df = self.file_df.withColumn(column_name, F.trunc('birth_date', 'year'))

    def deid_city(self, column_name='city'):
        self.file_df = self.file_df.withColumn(column_name, F.lit('Edison'))

    def deid_state(self, column_name='state'):
        self.file_df = self.file_df.withColumn(column_name, F.lit('NJ'))

    def deid_zip_code(self, column_name='zip'):
        self.file_df = self.file_df.withColumn(column_name, F.lit('08837'))

    def deid_phone_fax(self, column_name='phone'):
        self.file_df = self.file_df.withColumn(column_name, F.lit('732-271-0600'))

    def deid_age(self, column_name='age'):
        self.file_df = self.file_df.withColumn(
            'age',
            F.when(
                F.col(column_name).cast('int') > 89,
                F.lit(90)
            ).otherwise(
                F.col(column_name)
            )
        )

    def deid_death_date(
            self,
            column_name='death_date',
            eligibility_date_column_name='eligibility_date'
    ):
        self.file_df = self.file_df.withColumn(
            column_name,
            F.when(
                F.year(eligibility_date_column_name) < F.year(column_name),
                F.trunc(column_name, 'year')
            ).otherwise(
                F.make_date(F.year(column_name), F.lit(12), F.lit(31))
            )
        )

    # figure 4
    def _deid_basic_date(self, col_name):
        self.file_df = self.file_df.withColumn(
            f'{col_name}_deid',
            F.when(F.month(col_name) >= 6, F.date_sub(col_name, 30))
            .otherwise(F.date_add(col_name, 30))
        )

    def _deid_basic_and_depending_date(self, basic_col_name, depending_col_name):
        self._deid_basic_date(basic_col_name)
        self.file_df = self.file_df.withColumn(
            depending_col_name,
            F.date_add(f'{basic_col_name}_deid', F.datediff(depending_col_name, basic_col_name))
        )

    def deid_admission_date(self, column_name='admission_date'):
        self._deid_basic_date(column_name)

    def deid_admission_discharged_dates(
            self,
            adm_date_column_name='admission_date',
            disch_column_name='discharged_date'
    ):
        self._deid_basic_and_depending_date(adm_date_column_name, disch_column_name)

    def deid_service_from_date(self, column_name='service_from_date'):
        self._deid_basic_date(column_name)

    def deid_service_from_through_dates(
            self,
            srvc_from_column_name='service_from_date',
            srvc_through_column_name='service_through_date'
    ):
        self._deid_basic_and_depending_date(srvc_from_column_name, srvc_through_column_name)

    def deid_prescription_date(self, column_name='prescription_date'):
        self._deid_basic_date(column_name)

    def remove_identified_columns(self):
        """
        This method removes all columns that should be replaced with de_identified values.
        This method required because several methods (like deid_admission_discharged_dates)
        use both identified and de_identified value
        :return: None
        """
        deidentified_columns = [x for x in self.file_df.columns if (x.endswith('_deid'))]
        for column in deidentified_columns:
            self.file_df = self.file_df.withColumn(
                column.removesuffix('_deid'),
                F.col(column)
            ).drop(column)

    # Results conversion
    def convert_to_json(self):
        return self.file_df.toJSON()


