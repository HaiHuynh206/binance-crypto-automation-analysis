from __future__ import annotations

from crypto_ingest import cli


def test_unknown_silver_dataset_is_a_clean_error(capsys):
    code = cli.main(
        ["silver", "-d", "nope", "--start", "2026-10-20", "--end", "2026-10-21"]
    )

    assert code == 2
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "Unknown silver dataset" in err


def test_unknown_dq_status_is_a_clean_error(capsys):
    code = cli.main(["dq", "--fail-on", "nope"])

    assert code == 2
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "Unknown DQ status" in err
