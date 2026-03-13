from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_commands import (
    authors,
    create, create_dynamic_table, alter_table, read_table, write_table,
    start_transaction, commit_transaction,
    lookup_rows, select_rows, insert_rows, delete_rows,
    sync_create_cells, sync_mount_table, sync_flush_table, sync_compact_table, sync_unmount_table)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

import yt.yson as yson

from yt.yson import get_bytes

from yt.xdelta_aggregate_column.bindings import State
from yt.xdelta_aggregate_column.bindings import StateEncoder
from yt.xdelta_aggregate_column.bindings import XDeltaCodec


##################################################################


@pytest.mark.enabled_multidaemon
class TestAggregateColumns(TestSortedDynamicTablesBase):
    ENABLE_MULTIDAEMON = True

    def _create_table_with_aggregate_column(self, path, aggregate="sum", **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "time", "type": "int64"},
                        {"name": "value", "type": "int64", "aggregate": aggregate},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_aggregate_columns(self, optimize_for):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", optimize_for=optimize_for)
        sync_mount_table("//tmp/t")

        def verify_row(key, expected):
            actual = lookup_rows("//tmp/t", [{"key": key}])
            assert_items_equal(actual, expected)
            actual = select_rows("key, time, value from [//tmp/t]")
            assert_items_equal(actual, expected)

        def test_row(row, expected, **kwargs):
            insert_rows("//tmp/t", [row], **kwargs)
            verify_row(row["key"], [expected])

        def verify_after_flush(row):
            verify_row(row["key"], [row])
            assert_items_equal(read_table("//tmp/t"), [row])

        test_row(
            {"key": 1, "time": 1, "value": 10},
            {"key": 1, "time": 1, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 2, "value": 10},
            {"key": 1, "time": 2, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 3, "value": 10},
            {"key": 1, "time": 3, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 3, "value": 30})
        test_row(
            {"key": 1, "time": 4, "value": 10},
            {"key": 1, "time": 4, "value": 40},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 5, "value": 10},
            {"key": 1, "time": 5, "value": 50},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 6, "value": 10},
            {"key": 1, "time": 6, "value": 60},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 6, "value": 60})
        test_row(
            {"key": 1, "time": 7, "value": 10},
            {"key": 1, "time": 7, "value": 70},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 8, "value": 10},
            {"key": 1, "time": 8, "value": 80},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 9, "value": 10},
            {"key": 1, "time": 9, "value": 90},
            aggregate=True,
        )

        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row(
            {"key": 1, "time": 10, "value": 10},
            {"key": 1, "time": 10, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 11, "value": 10},
            {"key": 1, "time": 11, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 12, "value": 10},
            {"key": 1, "time": 12, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 12, "value": 30})
        test_row(
            {"key": 1, "time": 13, "value": 10},
            {"key": 1, "time": 13, "value": 40},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 14, "value": 10},
            {"key": 1, "time": 14, "value": 50},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 15, "value": 10},
            {"key": 1, "time": 15, "value": 60},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 15, "value": 60})
        delete_rows("//tmp/t", [{"key": 1}])
        verify_row(1, [])
        test_row(
            {"key": 1, "time": 16, "value": 10},
            {"key": 1, "time": 16, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 17, "value": 10},
            {"key": 1, "time": 17, "value": 20},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 18, "value": 10},
            {"key": 1, "time": 18, "value": 30},
            aggregate=True,
        )

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 18, "value": 30})
        test_row({"key": 1, "time": 19, "value": 10}, {"key": 1, "time": 19, "value": 10})
        test_row(
            {"key": 1, "time": 20, "value": 10},
            {"key": 1, "time": 20, "value": 20},
            aggregate=True,
        )
        test_row({"key": 1, "time": 21, "value": 10}, {"key": 1, "time": 21, "value": 10})

        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        verify_after_flush({"key": 1, "time": 21, "value": 10})

    @authors("savrus")
    def test_aggregate_min_max(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="min", optimize_for="scan")
        sync_mount_table("//tmp/t")

        insert_rows(
            "//tmp/t",
            [
                {"key": 1, "time": 1, "value": 10},
                {"key": 2, "time": 1, "value": 20},
                {"key": 3, "time": 1},
            ],
            aggregate=True,
        )
        insert_rows(
            "//tmp/t",
            [
                {"key": 1, "time": 2, "value": 30},
                {"key": 2, "time": 2, "value": 40},
                {"key": 3, "time": 2},
            ],
            aggregate=True,
        )
        assert_items_equal(select_rows("max(value) as max from [//tmp/t] group by 1"), [{"max": 20}])

    @authors("savrus")
    def test_aggregate_first(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="first")
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]

    @authors("savrus")
    @pytest.mark.parametrize("aggregate", ["min", "max", "sum", "first"])
    def test_aggregate_update(self, aggregate):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "time": 1}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 1, "value": None}]
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 10}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 10}]
        insert_rows("//tmp/t", [{"key": 1, "time": 3}], aggregate=True)
        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 3, "value": 10}]

    @authors("savrus")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_aggregate_alter(self, optimize_for):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "int64"},
        ]
        create("table", "//tmp/t", attributes={"dynamic": True, "schema": schema, "optimize_for": optimize_for})
        sync_mount_table("//tmp/t")

        def verify_row(key, expected):
            actual = lookup_rows("//tmp/t", [{"key": key}])
            assert_items_equal(actual, expected)
            actual = select_rows("key, time, value from [//tmp/t]")
            assert_items_equal(actual, expected)

        def test_row(row, expected, **kwargs):
            insert_rows("//tmp/t", [row], **kwargs)
            verify_row(row["key"], [expected])

        test_row(
            {"key": 1, "time": 1, "value": 10},
            {"key": 1, "time": 1, "value": 10},
            aggregate=True,
        )
        test_row(
            {"key": 1, "time": 2, "value": 20},
            {"key": 1, "time": 2, "value": 20},
            aggregate=True,
        )

        sync_unmount_table("//tmp/t")
        schema[2]["aggregate"] = "sum"
        alter_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        verify_row(1, [{"key": 1, "time": 2, "value": 20}])
        test_row(
            {"key": 1, "time": 3, "value": 10},
            {"key": 1, "time": 3, "value": 30},
            aggregate=True,
        )

    @authors("savrus")
    def test_aggregate_non_atomic(self):
        sync_create_cells(1)
        self._create_table_with_aggregate_column("//tmp/t", aggregate="sum", atomicity="none")
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet", atomicity="none")
        tx2 = start_transaction(type="tablet", atomicity="none")

        insert_rows(
            "//tmp/t",
            [{"key": 1, "time": 1, "value": 10}],
            aggregate=True,
            atomicity="none",
            tx=tx1,
        )
        insert_rows(
            "//tmp/t",
            [{"key": 1, "time": 2, "value": 20}],
            aggregate=True,
            atomicity="none",
            tx=tx2,
        )

        commit_transaction(tx1)
        commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}]) == [{"key": 1, "time": 2, "value": 30}]

    @pytest.mark.parametrize(
        "merge_rows_on_flush, min_data_ttl, min_data_versions",
        [a + b for a in [(False,), (True,)] for b in [(0, 0), (1, 1)]],
    )
    @authors("babenko")
    def test_aggregate_merge_rows_on_flush(self, merge_rows_on_flush, min_data_ttl, min_data_versions):
        sync_create_cells(1)
        self._create_table_with_aggregate_column(
            "//tmp/t",
            merge_rows_on_flush=merge_rows_on_flush,
            min_data_ttl=min_data_ttl,
            min_data_versions=min_data_versions,
            max_data_ttl=1000000,
            max_data_versions=1,
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 1000}], aggregate=False)
        delete_rows("//tmp/t", [{"key": 1}])
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 2000}], aggregate=True)
        delete_rows("//tmp/t", [{"key": 1}])
        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 10}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 20}], aggregate=True)

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 30}])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 30}])

        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": 100}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": 200}], aggregate=True)

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

        sync_compact_table("//tmp/t")

        assert_items_equal(select_rows("* from [//tmp/t]"), [{"key": 1, "time": 2, "value": 330}])

    @authors("savrus")
    @pytest.mark.parametrize("aggregate", ["avg", "cardinality"])
    def test_invalid_aggregate(self, aggregate):
        sync_create_cells(1)
        with pytest.raises(YtError):
            self._create_table_with_aggregate_column("//tmp/t", aggregate=aggregate)

    @authors("abatovkin")
    def test_aggregate_hll_overwrite(self):
        """Test that writing without aggregate flag overwrites the HLL state."""
        sync_create_cells(1)

        precision = 14
        register_count = 1 << precision

        def make_empty_hll():
            return b"\x00" * register_count

        def hll_add(state, fingerprint):
            registers = bytearray(state)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if registers[index] < zeroes_plus_one:
                registers[index] = zeroes_plus_one
            return bytes(registers)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": "hll_14_merge_state"},
        ]
        create_dynamic_table("//tmp/t_hll_overwrite", schema=schema)
        sync_mount_table("//tmp/t_hll_overwrite")

        hll1 = make_empty_hll()
        for i in range(100):
            hll1 = hll_add(hll1, i * 997)

        insert_rows("//tmp/t_hll_overwrite", [{"key": 1, "hll_state": hll1}], aggregate=True)

        hll2 = make_empty_hll()
        for i in range(5):
            hll2 = hll_add(hll2, i * 31)

        insert_rows("//tmp/t_hll_overwrite", [{"key": 1, "hll_state": hll2}])

        rows = lookup_rows("//tmp/t_hll_overwrite", [{"key": 1}])
        assert len(rows) == 1
        actual_state = get_bytes(rows[0]["hll_state"])
        assert actual_state == hll2


    @authors("abatovkin")
    @pytest.mark.parametrize("precision", [7, 14])
    def test_aggregate_hll_cardinality_estimation(self, precision):
        """End-to-end test: build HLL from raw values, insert as aggregate column,
        merge across multiple writes/flush/compaction, and verify cardinality estimate."""
        import hashlib
        import math

        sync_create_cells(1)

        register_count = 1 << precision

        def farm_hash(value):
            h = hashlib.md5(str(value).encode()).digest()
            return int.from_bytes(h[:8], "little")

        def make_empty_hll():
            return bytearray(register_count)

        def hll_add(state, raw_value):
            fingerprint = farm_hash(raw_value)
            fingerprint |= (1 << 63)
            index = fingerprint & (register_count - 1)
            shifted = fingerprint >> precision
            zeroes_plus_one = 0
            while zeroes_plus_one < 64 and (shifted & 1) == 0:
                shifted >>= 1
                zeroes_plus_one += 1
            zeroes_plus_one += 1
            if state[index] < zeroes_plus_one:
                state[index] = zeroes_plus_one

        def hll_estimate(state):
            m = register_count
            alpha = 0.7213 / (1.0 + 1.079 / m)
            raw = alpha * m * m / sum(2.0 ** (-r) for r in state)
            # small range correction
            zeros = sum(1 for r in state if r == 0)
            if raw <= 2.5 * m and zeros > 0:
                return m * math.log(m / zeros)
            return raw

        aggregate_name = "hll_{}_merge_state".format(precision)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": aggregate_name},
        ]
        create_dynamic_table("//tmp/t_hll_card", schema=schema)
        sync_mount_table("//tmp/t_hll_card")

        # Simulate 3 batches of user visits:
        # batch 1: users 0..999 (1000 unique)
        # batch 2: users 500..1499 (500 new, 500 overlap)
        # batch 3: users 1000..1999 (500 new, 500 overlap)
        # Total unique: 2000
        batches = [
            range(0, 1000),
            range(500, 1500),
            range(1000, 2000),
        ]

        for batch in batches:
            hll = make_empty_hll()
            for user_id in batch:
                hll_add(hll, user_id)
            insert_rows("//tmp/t_hll_card", [{"key": 1, "hll_state": bytes(hll)}], aggregate=True)

        rows = lookup_rows("//tmp/t_hll_card", [{"key": 1}])
        estimate = hll_estimate(get_bytes(rows[0]["hll_state"]))
        # HLL with precision 7 (128 registers) has ~9% standard error,
        # but our simplified estimator (no bias correction) can deviate more.
        # HLL with precision 14 (16384 registers) has ~0.8% standard error.
        max_error = 0.20 if precision == 7 else 0.05
        assert abs(estimate - 2000) / 2000 < max_error, \
            "Cardinality estimate {} is too far from expected 2000 (error: {:.1%})".format(
                estimate, abs(estimate - 2000) / 2000)

        sync_flush_table("//tmp/t_hll_card")
        sync_compact_table("//tmp/t_hll_card")

        rows = lookup_rows("//tmp/t_hll_card", [{"key": 1}])
        estimate_after_compact = hll_estimate(get_bytes(rows[0]["hll_state"]))
        assert estimate_after_compact == estimate, \
            "Cardinality changed after compaction: {} -> {}".format(estimate, estimate_after_compact)

    @authors("leasid")
    def test_aggregate_xdelta(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "time", "type": "int64"},
            {"name": "value", "type": "string", "aggregate": "xdelta"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        encoder = StateEncoder(None)
        codec = XDeltaCodec(None)

        # basic case: write patches
        base = b""
        state = b"123456"
        patch = encoder.create_patch_state((base, state))
        insert_rows("//tmp/t", [{"key": 1, "time": 1, "value": patch}], aggregate=True)

        state1 = b"567890"
        patch = encoder.create_patch_state((state, state1))
        insert_rows("//tmp/t", [{"key": 1, "time": 2, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.PATCH_TYPE
        result_state = codec.apply_patch((base, result.payload_data, len(state1)))
        assert result_state == state1

        state2 = b"7890"
        patch = encoder.create_patch_state((state1, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 3, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.PATCH_TYPE
        result_state = codec.apply_patch((base, result.payload_data, len(state2)))
        assert result_state == state2

        # overwrite state
        base = state
        base_state = encoder.create_base_state(base)
        insert_rows("//tmp/t", [{"key": 1, "time": 4, "value": base_state}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == base

        patch = encoder.create_patch_state((base, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 5, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # test null as patch
        patch = encoder.create_patch_state((state2, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 6, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # plant error
        patch = encoder.create_patch_state((state1, state2))  # inconsistent patch - not applicable for stored base
        insert_rows("//tmp/t", [{"key": 1, "time": 7, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.ERROR_TYPE
        assert result.has_error_code
        assert result.error_code > 0  # base hash error

        # fix error
        base_state = encoder.create_base_state(base)
        insert_rows("//tmp/t", [{"key": 1, "time": 8, "value": base_state}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == base

        patch = encoder.create_patch_state((base, state2))
        insert_rows("//tmp/t", [{"key": 1, "time": 9, "value": patch}], aggregate=True)

        row = lookup_rows("//tmp/t", [{"key": 1}])
        result = State(get_bytes(row[0]["value"]))
        assert result.type == result.BASE_TYPE
        assert result.payload_data == state2

        # delete rows
        delete_rows("//tmp/t", [{"key": 1}])
        row = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(row, [])

    @authors("hitsedesen")
    def test_aggregate_dict_sum(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "dict_sum"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        insert_rows(
            "//tmp/t", [{"key": 1}, {"key": 2, "value": {"a": 11, "b": {"c": {"d": 7}}, "e": {"f": {"g": 13}}, "h": 5}}]
        )
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [
            {"key": 1, "value": yson.YsonEntity()},
            {"key": 2, "value": {"a": 11, "b": {"c": {"d": 7}}, "e": {"f": {"g": 13}}, "h": 5}},
        ]
        insert_rows(
            "//tmp/t",
            [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"a": 3, "b": {"c": {"d": 17}}}}],
            aggregate=True,
        )
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [
            {"key": 1, "value": {"a": 3}},
            {"key": 2, "value": {"a": 14, "b": {"c": {"d": 24}}, "e": {"f": {"g": 13}}, "h": 5}},
        ]
        insert_rows("//tmp/t", [{"key": 2, "value": {"a": -14, "b": {"c": {"d": -24}}}}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"e": {"f": {"g": 13}}, "h": 5}}]
        insert_rows("//tmp/t", [{"key": 2, "value": {"h": 25, "q": 1}}])
        value = lookup_rows("//tmp/t", [{"key": 1}, {"key": 2}])
        assert value == [{"key": 1, "value": {"a": 3}}, {"key": 2, "value": {"h": 25, "q": 1}}]

    @authors("aleksandra-zh")
    def test_aggregate_stored_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_stored_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]],
            []
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(20), 1, yson.YsonUint64(2)]]
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(30), 3, yson.YsonUint64(4)]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]],
            []
        ])}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == [[yson.YsonUint64(20), 1, yson.YsonUint64(2)], [yson.YsonUint64(30), 3, yson.YsonUint64(4)]]

        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList([
            [],
            [[yson.YsonUint64(30), 3, yson.YsonUint64(4)], [yson.YsonUint64(20), 1, yson.YsonUint64(2)]]
        ])}], aggregate=True)

        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == []

    @authors("aleksandra-zh")
    def test_aggregate_last_seen_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_last_seen_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        original_rows = [
            [yson.YsonUint64(i), 16, yson.YsonUint64(2)] for i in range(3)
        ]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(original_rows)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == original_rows

        new_row = [[yson.YsonUint64(20), 16, yson.YsonUint64(2)]]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == original_rows[1:] + new_row

    @authors("aleksandra-zh")
    def test_aggregate_erasure_last_seen_replica_set(self):
        sync_create_cells(1)
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "any", "aggregate": "_yt_last_seen_replica_set"},
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        rows = [
            [yson.YsonUint64(i), i, yson.YsonUint64(2)] for i in range(16)
        ]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(rows)}], aggregate=True)
        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        assert value == rows

        new_row_3 = [[yson.YsonUint64(20), 3, yson.YsonUint64(2)]]
        new_row_5 = [[yson.YsonUint64(30), 5, yson.YsonUint64(2)]]
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row_3)}], aggregate=True)
        insert_rows("//tmp/t", [{"key": 1, "value": yson.YsonList(new_row_5)}], aggregate=True)

        value = lookup_rows("//tmp/t", [{"key": 1}])[0]["value"]
        rows[3] = new_row_3[0]
        rows[5] = new_row_5[0]
        assert value == rows

    @authors("abatovkin")
    @authors("abatovkin")
    def test_aggregate_uniq_merge_state(self):
        """Test uniq_merge_state aggregate columns for state-based cardinality estimation."""
        sync_create_cells(1)

        create("table", "//tmp/raw_data", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "int64"},
            ]
        })

        write_table("//tmp/raw_data", [
            {"key": 1, "value": 1},
            {"key": 1, "value": 2}, 
            {"key": 1, "value": 3},
            {"key": 1, "value": 1},  # duplicate
            {"key": 1, "value": 2},  # duplicate
            {"key": 2, "value": 4},
            {"key": 2, "value": 5},
            {"key": 2, "value": 6},
            {"key": 2, "value": 4},  # duplicate
        ])

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "uniq_state", "type": "string", "aggregate": "uniq_merge_state"},
        ]
        create_dynamic_table("//tmp/t_uniq", schema=schema)
        sync_mount_table("//tmp/t_uniq")

        states = select_rows("key, uniq_state(value) as state from [//tmp/raw_data] group by key")
        
        for state in states:
            insert_rows("//tmp/t_uniq", [{"key": state["key"], "uniq_state": state["state"]}], aggregate=True)

        rows = lookup_rows("//tmp/t_uniq", [{"key": 1}, {"key": 2}])
        assert len(rows) == 2

        cardinalities = select_rows("key, uniq_merge(uniq_state) as cardinality from [//tmp/t_uniq] group by key order by key")
        
        assert len(cardinalities) == 2
        assert cardinalities[0]["key"] == 1
        assert cardinalities[1]["key"] == 2
        
        # For uniq algorithm, exact cardinality should be returned for small sets
        # key=1 should have 3 unique values (1,2,3), key=2 should have 3 unique values (4,5,6)
        initial_card1 = cardinalities[0]["cardinality"]
        initial_card2 = cardinalities[1]["cardinality"]
        assert initial_card1 == 3
        assert initial_card2 == 3

        write_table("//tmp/raw_data_2", [
            {"key": 1, "value": 4},  # new unique value for key=1
            {"key": 1, "value": 3},  # duplicate 
            {"key": 2, "value": 7},  # new unique value for key=2
            {"key": 2, "value": 8},  # new unique value for key=2
        ])
        
        additional_states = select_rows("key, uniq_state(value) as state from [//tmp/raw_data_2] group by key")
        
        for state in additional_states:
            insert_rows("//tmp/t_uniq", [{"key": state["key"], "uniq_state": state["state"]}], aggregate=True)

        updated_cardinalities = select_rows("key, uniq_merge(uniq_state) as cardinality from [//tmp/t_uniq] group by key order by key")
        
        assert len(updated_cardinalities) == 2
        assert updated_cardinalities[0]["cardinality"] == 4
        assert updated_cardinalities[1]["cardinality"] == 5

        sync_flush_table("//tmp/t_uniq")
        sync_compact_table("//tmp/t_uniq")

        final_cardinalities = select_rows("key, uniq_merge(uniq_state) as cardinality from [//tmp/t_uniq] group by key order by key")
        assert final_cardinalities[0]["cardinality"] == 4
        assert final_cardinalities[1]["cardinality"] == 5

        write_table("//tmp/raw_data_3", [
            {"key": 1, "value": 3},  # duplicate with existing
            {"key": 1, "value": 5},  # new unique value
        ])
        
        overlap_state = select_rows("key, uniq_state(value) as state from [//tmp/raw_data_3] group by key")[0]
        insert_rows("//tmp/t_uniq", [{"key": overlap_state["key"], "uniq_state": overlap_state["state"]}], aggregate=True)

        final_check = select_rows("key, uniq_merge(uniq_state) as cardinality from [//tmp/t_uniq] where key = 1")[0]
        assert final_check["cardinality"] == 5

    @authors("abatovkin")
    def test_aggregate_uniq_direct(self):
        """Test direct uniq aggregate column (uniq function without state)."""
        sync_create_cells(1)

        create("table", "//tmp/raw_data", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "int64"},
            ]
        })

        write_table("//tmp/raw_data", [
            {"key": 1, "value": 1},
            {"key": 1, "value": 2}, 
            {"key": 1, "value": 3},
            {"key": 1, "value": 1},  # duplicate
            {"key": 2, "value": 4},
            {"key": 2, "value": 5},
        ])

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "uniq_count", "type": "uint64", "aggregate": "uniq"},
        ]
        create_dynamic_table("//tmp/t_uniq_basic", schema=schema)
        sync_mount_table("//tmp/t_uniq_basic")

        cardinalities = select_rows("key, uniq(value) as count from [//tmp/raw_data] group by key")
        
        for card in cardinalities:
            insert_rows("//tmp/t_uniq_basic", [{"key": card["key"], "uniq_count": card["count"]}], aggregate=True)

        rows = lookup_rows("//tmp/t_uniq_basic", [{"key": 1}, {"key": 2}])
        assert len(rows) == 2
        
        # Sort by key for consistent comparison
        rows = sorted(rows, key=lambda x: x["key"])
        
        # For uniq algorithm, exact cardinality should be returned for small sets
        assert rows[0]["key"] == 1 and rows[0]["uniq_count"] == 3  # values 1,2,3
        assert rows[1]["key"] == 2 and rows[1]["uniq_count"] == 2  # values 4,5

    @authors("abatovkin")
    def test_aggregate_uniq_state_and_merge(self):
        """Test uniq_state and uniq_merge functions for state-based workflows."""
        sync_create_cells(1)

        create("table", "//tmp/raw_data", attributes={
            "schema": [
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "int64"},
            ]
        })

        write_table("//tmp/raw_data", [
            {"key": 1, "value": 1},
            {"key": 1, "value": 2}, 
            {"key": 1, "value": 3},
            {"key": 1, "value": 1},  # duplicate
            {"key": 2, "value": 4},
            {"key": 2, "value": 5},
            {"key": 2, "value": 6},
        ])

        schema_state = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "uniq_state", "type": "string", "aggregate": "uniq_state"},
        ]
        create_dynamic_table("//tmp/t_uniq_state", schema=schema_state)
        sync_mount_table("//tmp/t_uniq_state")

        states = select_rows("key, uniq_state(value) as state from [//tmp/raw_data] group by key")
        
        for state in states:
            insert_rows("//tmp/t_uniq_state", [{"key": state["key"], "uniq_state": state["state"]}], aggregate=True)

        rows = lookup_rows("//tmp/t_uniq_state", [{"key": 1}, {"key": 2}])
        assert len(rows) == 2

        schema_merge = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "uniq_count", "type": "uint64", "aggregate": "uniq_merge"},
        ]
        create_dynamic_table("//tmp/t_uniq_merge", schema=schema_merge)
        sync_mount_table("//tmp/t_uniq_merge")

        states = select_rows("key, uniq_state from [//tmp/t_uniq_state]")
        
        for state in states:
            insert_rows("//tmp/t_uniq_merge", [{"key": state["key"], "uniq_count": state["uniq_state"]}], aggregate=True)

        final_counts = lookup_rows("//tmp/t_uniq_merge", [{"key": 1}, {"key": 2}])
        final_counts = sorted(final_counts, key=lambda x: x["key"])
        
        assert final_counts[0]["key"] == 1 and final_counts[0]["uniq_count"] == 3
        assert final_counts[1]["key"] == 2 and final_counts[1]["uniq_count"] == 3

    @authors("abatovkin") 
    @pytest.mark.parametrize("precision", [7, 14])
    def test_aggregate_hll_all_variants(self, precision):
        """Test all HLL function variants: hll, hll_state, hll_merge, hll_merge_state."""
        sync_create_cells(1)

        schema_raw = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "int64"},
        ]
        create_dynamic_table("//tmp/raw_hll_data", schema=schema_raw)
        sync_mount_table("//tmp/raw_hll_data")

        test_data = [
            {"key": 1, "value": i} for i in range(100)
        ] + [
            {"key": 2, "value": i} for i in range(50, 150)
        ]
        insert_rows("//tmp/raw_hll_data", test_data)

        counts = select_rows(f"key, hll_{precision}(value) as count from [//tmp/raw_hll_data] group by key order by key")
        assert len(counts) == 2
        assert abs(counts[0]["count"] - 100) < 20  # Allow HLL estimation error
        assert abs(counts[1]["count"] - 100) < 20

        schema_state = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": f"hll_{precision}_state"},
        ]
        create_dynamic_table("//tmp/t_hll_state", schema=schema_state)
        sync_mount_table("//tmp/t_hll_state")

        states = select_rows(f"key, hll_{precision}_state(value) as state from [//tmp/raw_hll_data] group by key")
        for state in states:
            insert_rows("//tmp/t_hll_state", [{"key": state["key"], "hll_state": state["state"]}], aggregate=True)

        schema_merge = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_count", "type": "uint64", "aggregate": f"hll_{precision}_merge"},
        ]
        create_dynamic_table("//tmp/t_hll_merge", schema=schema_merge)
        sync_mount_table("//tmp/t_hll_merge")

        states = select_rows("key, hll_state from [//tmp/t_hll_state]")
        for state in states:
            insert_rows("//tmp/t_hll_merge", [{"key": state["key"], "hll_count": state["hll_state"]}], aggregate=True)

        merge_counts = lookup_rows("//tmp/t_hll_merge", [{"key": 1}, {"key": 2}])
        merge_counts = sorted(merge_counts, key=lambda x: x["key"])
        
        assert abs(merge_counts[0]["hll_count"] - 100) < 20
        assert abs(merge_counts[1]["hll_count"] - 100) < 20

        schema_merge_state = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "hll_state", "type": "string", "aggregate": f"hll_{precision}_merge_state"},
        ]
        create_dynamic_table("//tmp/t_hll_merge_state", schema=schema_merge_state)
        sync_mount_table("//tmp/t_hll_merge_state")

        original_states = select_rows("key, hll_state from [//tmp/t_hll_state]")
        for state in original_states:
            insert_rows("//tmp/t_hll_merge_state", [{"key": state["key"], "hll_state": state["hll_state"]}], aggregate=True)

        final_states = select_rows("key, hll_state from [//tmp/t_hll_merge_state]")
        for state in final_states:
            final_count = select_rows(f"hll_{precision}_merge('{state['hll_state']}') as count")[0]["count"]
            assert abs(final_count - 100) < 20

##################################################################


@pytest.mark.enabled_multidaemon
class TestAggregateColumnsMulticell(TestAggregateColumns):
    ENABLE_MULTIDAEMON = True
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }


@pytest.mark.enabled_multidaemon
class TestAggregateColumnsRpcProxy(TestAggregateColumns):
    ENABLE_MULTIDAEMON = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
