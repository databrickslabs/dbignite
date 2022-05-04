def omop_cdm_to_person_dashboard(cdm_database: str, mapping_database: str) -> PersonDashboard:
  spark.sql(f'USE {cdm_database}')
  person_df = spark.read.table(PERSON_TABLE)
  condition_df = spark.read.table(CONDITION_TABLE)
  procedure_occurrence_df = spark.read.table(PROCEDURE_OCCURRENCE_TABLE)
  
  encounter_df = spark.read.table(ENCOUNTER_TABLE)
  
  condition_summary_df = _summarize_condition(condition_df)
  procedure_occurrence_summary_df = _summarize_procedure_occurrence(procedure_occurrence_df)
                                  
  encounter_summary_df = _summarize_encounter(encounter_df)
  
  return PersonDashboard(
    person_df
    .join(condition_summary_df, 'person_id', 'left')
    .join(procedure_occurrence_summary_df, 'person_id', 'left')
    .join(encounter_summary_df, 'person_id', 'left')
  )
  
def _summarize_condition(condition_df):
  return (
    condition_df
    .orderBy('condition_start_datetime')
    .select(col('person_id'), struct('*').alias('condition'))
    .groupBy('person_id')
    .agg(
      collect_list('condition').alias('conditions'),
    )
  )

def _summarize_procedure_occurrence(condition_df):
  return (
    condition_df
    .orderBy('procedure_start_date')
    .select(col('person_id'), struct('*').alias('procedure_occurrence'))
    .groupBy('person_id')
    .agg(
      collect_list('procedure_occurrence').alias('procedure_occurrences'),
    )
  )

def _summarize_encounter(encounter_df):
  return (
    encounter_df
    .orderBy('encounter_period_start')
    .select(col('person_id'), struct('*').alias('encounter'))
    .groupBy('person_id')
    .agg(
      collect_list('encounter').alias('encounters'),
    )
  )

