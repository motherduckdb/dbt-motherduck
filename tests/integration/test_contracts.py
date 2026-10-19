import pytest
from dbt.tests.util import relation_from_name, run_dbt


class TestContracts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "typed.sql": """
{{ config(materialized=var('kind'), unique_key='id', on_schema_change='fail', contract={'enforced': true}) }}
select 1 as id, {{ "42" if var('wrong_type', false) else "'value'" }} as label, 3 as "order id"
""",
            "schema.yml": """
version: 2
models:
  - name: typed
    columns:
      - name: id
        data_type: integer
      - name: label
        data_type: varchar
      - name: order id
        data_type: integer
        quote: true
""",
        }

    @pytest.mark.parametrize("kind", ["table", "view", "incremental"])
    def test_contract_and_mismatch(self, project, kind):
        args = ["run", "--vars", f"{{kind: {kind}}}"]
        run_dbt([*args, "--full-refresh"])
        run_dbt(args)
        target = relation_from_name(project.adapter, "typed")
        assert project.run_sql(f"SELECT * FROM {target}", fetch="all") == [
            (1, "value", 3)
        ]
        result = run_dbt(
            ["run", "--full-refresh", "--vars", f"{{kind: {kind}, wrong_type: true}}"],
            expect_pass=False,
        )
        assert "contract" in result.results[0].message.lower()
        assert project.run_sql(f"SELECT * FROM {target}", fetch="all") == [
            (1, "value", 3)
        ]


class TestStructContracts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "nested.sql": """
{{ config(materialized=var('kind'), contract={'enforced': true}) }}
{% set payloads = {
    'match': "{'a': 1, 'b': 'x'}",
    'field_type': "{'a': 1::bigint, 'b': 'x'}",
    'field_order': "{'b': 'x', 'a': 1}",
} %}
select 1 as id, {{ payloads[var('payload', 'match')] }} as payload
""",
            "dotted.sql": """
{{ config(materialized=var('kind'), contract={'enforced': true}) }}
select 1 as id, {'a': 1, 'b': 'x'} as payload
""",
            "schema.yml": """
version: 2
models:
  - name: nested
    columns:
      - name: id
        data_type: integer
      - name: payload
        data_type: struct(a integer, b varchar)
  - name: dotted
    columns:
      - name: id
        data_type: integer
      - name: payload.a
        data_type: integer
        quote: true
      - name: payload.b
        data_type: varchar
        quote: true
""",
        }

    @pytest.mark.parametrize("kind", ["table", "view"])
    def test_struct_contract(self, project, kind):
        run_dbt(["run", "--select", "nested", "--vars", f"{{kind: {kind}}}"])
        for payload in ["field_type", "field_order"]:
            result = run_dbt(
                [
                    "run",
                    "--select",
                    "nested",
                    "--vars",
                    f"{{kind: {kind}, payload: {payload}}}",
                ],
                expect_pass=False,
            )
            assert "contract" in result.results[0].message.lower()

    @pytest.mark.parametrize("kind", ["table", "view"])
    def test_dotted_columns_do_not_match_struct(self, project, kind):
        result = run_dbt(
            ["run", "--select", "dotted", "--vars", f"{{kind: {kind}}}"],
            expect_pass=False,
        )
        assert "contract" in result.results[0].message.lower()
