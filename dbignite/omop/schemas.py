from pyspark.sql.types import *

ENTRY_SCHEMA = StructType(
    [
        StructField(
            "resource",
            StructType(
                [
                    StructField("id", StringType()),
                    # Not named in the spec.
                    StructField("resourceType", StringType()),
                ]
            ),
        ),
        StructField(
            "request",
            StructType(
                [
                    # Not technically required, but named in spec.
                    StructField("url", StringType())
                ]
            ),
        ),
    ]
)

JSON_ENTRY_SCHEMA = StructType(
    [StructField("entry_json", StringType()), StructField("entry", ENTRY_SCHEMA)]
)

PERSON_SCHEMA = StructType(
    [
        StructField("person_id", StringType()),
        StructField("name", StringType()),
        StructField("gender_source_value", StringType()),
        StructField("year_of_birth", IntegerType()),
        StructField("month_of_birth", IntegerType()),
        StructField("day_of_birth", IntegerType()),
        StructField("address", StringType()),
    ]
)

CONDITION_SCHEMA = StructType(
    [
        StructField("condition_occurrence_id", StringType()),
        StructField("person_id", StringType()),
        StructField("visit_occurrence_id", StringType()),
        StructField("condition_start_datetime", TimestampType()),
        StructField("condition_end_datetime", TimestampType()),
        StructField("condition_status", StringType()),
        StructField("condition_code", StringType()),
    ]
)

PROCEDURE_OCCURRENCE_SCHEMA = StructType(
    [
        StructField("procedure_occurrence_id", StringType()),
        StructField("person_id", StringType()),
        StructField("procedure_code", StringType()),
        StructField("procedure_code_display", StringType()),
        StructField("procedure_code_system", StringType()),
        StructField("procedure_start_date", DateType()),
        StructField("procedure_end_date", DateType()),
        StructField("procedure_type", StringType()),
        StructField("provider_id", StringType()),
        StructField("visit_occurrence_id", StringType()),
        StructField("location_id", StringType()),
        StructField("location_display", StringType()),
    ]
)

PARTICIPANT_SCHEMA = StructType(
    [
        StructField(
            "type",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "coding",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("code", StringType()),
                                        StructField("display", StringType()),
                                        StructField("system", StringType()),
                                    ]
                                )
                            ),
                        ),
                        StructField("text", StringType()),
                    ]
                )
            ),
        )
    ]
)

IDENTIFIER_SCHEMA = StructType(
    [
        StructField("use", StringType()),
        StructField("system", StringType()),
        StructField("value", StringType()),
    ]
)

LOCATION_SCHEMA = StructType(
    [
        StructField(
            "location",
            StructType(
                [
                    StructField("reference", StringType()),
                    StructField("display", StringType()),
                ]
            ),
        )
    ]
)

ENCOUNTER_SCHEMA = StructType(
    [
        StructField("encounter_id", StringType()),
        StructField("person_id", StringType()),
        StructField("encounter_period_start", TimestampType()),
        StructField("encounter_period_end", TimestampType()),
        StructField("serviceProvider", StringType()),
        StructField("encounter_status", StringType()),
        StructField("encounter_code", StringType()),
        StructField("encounter_status_text", StringType()),
        StructField("participant", ArrayType(PARTICIPANT_SCHEMA)),
        StructField("status", StringType()),
        StructField("identifier", ArrayType(IDENTIFIER_SCHEMA)),
        StructField("location", ArrayType(LOCATION_SCHEMA)),
    ]
)

CODING_SCHEMA = StructType(
    [
        StructField("code", StringType()),
        StructField("display", StringType()),
        StructField("system", StringType()),
    ]
)

CONDITION_SUMMARY_SCHEMA = StructType(
    [StructField("conditions", ArrayType(CONDITION_SCHEMA, "false"))]
)
