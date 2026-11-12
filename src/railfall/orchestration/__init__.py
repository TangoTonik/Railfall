"""Airflow-facing task callables.

The DAG files under `dags/` stay thin: every task calls a function from this
package, which only depends on the railfall code and can be unit-tested
without an Airflow install.
"""
