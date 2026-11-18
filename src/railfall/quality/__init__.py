"""Great Expectations checks on the warehouse `raw` layer (what the Spark loader writes).

`suites` declares the expectations as plain data (no GX import), `checkpoint`
turns them into a GX checkpoint and runs it against a SQL database.
"""
