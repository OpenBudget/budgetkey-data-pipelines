import dataflows as DF

def flow(parameters, *_):
    return DF.Flow(
        DF.dump_to_sql(
            parameters['tables'],
            engine=parameters.get('engine', 'env://DPP_DB_ENGINE'),
            updated_column=parameters.get("updated_column"),
            updated_id_column=parameters.get("updated_id_column"),
            batch_size=1
        )
    )
