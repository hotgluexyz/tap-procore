"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_procore.tap import TapProcore

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "client_id": "8bf080126bdcd81f46b5be811b8a71fc7177a37745c60ffefd6be45608b0c372",
    "client_secret": "9bc43ac6a406248cec9acb6987df38b505146993b8da4f8ecefad2fba690b1b7",
    "redirect_uri": "https://oauth.pstmn.io/v1/callback",
    "refresh_token": "b28cc7d3597dc1b0002d74ff97db18f961cb7d962d7e55995c7e79b9ff694cfe",
    "is_sandbox": True
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapProcore,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
