
def test_valid(cldf_dataset, cldf_sqlite_database, cldf_logger):
    assert cldf_sqlite_database
    assert cldf_dataset.validate(log=cldf_logger)
