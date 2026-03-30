"""Unit tests for renew_entitlements.py — VPN, autofix, and core logic."""

import subprocess
from unittest.mock import patch, MagicMock

import pytest

from renew_entitlements import (
    is_vpn_connected,
    connect_vpn,
    should_autofix,
    build_autofix_prompt,
    SKIP_AUTOFIX_PATTERNS,
    VPN_CONNECTION_NAME,
)


# ── VPN: is_vpn_connected ───────────────────────────────────────────────────

class TestIsVpnConnected:
    @patch("renew_entitlements.subprocess.run")
    def test_connected(self, mock_run):
        mock_run.return_value = MagicMock(stdout="Connected\n", returncode=0)
        assert is_vpn_connected() is True
        cmd = mock_run.call_args[0][0]
        assert "Get-VpnConnection" in " ".join(cmd)

    @patch("renew_entitlements.subprocess.run")
    def test_disconnected(self, mock_run):
        mock_run.return_value = MagicMock(stdout="Disconnected\n", returncode=0)
        assert is_vpn_connected() is False

    @patch("renew_entitlements.subprocess.run")
    def test_empty_output(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", returncode=1)
        assert is_vpn_connected() is False

    @patch("renew_entitlements.subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 10))
    def test_timeout_returns_false(self, mock_run):
        assert is_vpn_connected() is False

    @patch("renew_entitlements.subprocess.run", side_effect=FileNotFoundError)
    def test_powershell_not_found(self, mock_run):
        assert is_vpn_connected() is False


# ── VPN: connect_vpn ────────────────────────────────────────────────────────

class TestConnectVpn:
    @patch("renew_entitlements.is_vpn_connected", return_value=True)
    def test_already_connected_skips_rasdial(self, mock_check):
        assert connect_vpn() is True
        mock_check.assert_called_once()

    @patch("renew_entitlements.subprocess.run")
    @patch("renew_entitlements.is_vpn_connected", return_value=False)
    def test_successful_connect(self, mock_check, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="Successfully connected", stderr="")
        assert connect_vpn() is True
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == ["rasdial", VPN_CONNECTION_NAME]

    @patch("renew_entitlements.subprocess.run")
    @patch("renew_entitlements.is_vpn_connected", return_value=False)
    def test_rasdial_failure(self, mock_check, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="Error", stderr="")
        assert connect_vpn() is False

    @patch("renew_entitlements.subprocess.run", side_effect=subprocess.TimeoutExpired("rasdial", 30))
    @patch("renew_entitlements.is_vpn_connected", return_value=False)
    def test_rasdial_timeout(self, mock_check, mock_run):
        assert connect_vpn() is False

    @patch("renew_entitlements.subprocess.run", side_effect=OSError("no such file"))
    @patch("renew_entitlements.is_vpn_connected", return_value=False)
    def test_rasdial_not_found(self, mock_check, mock_run):
        assert connect_vpn() is False


# ── Autofix: should_autofix ─────────────────────────────────────────────────

class TestShouldAutofix:
    def test_success_returns_false(self):
        assert should_autofix({"status": "success"}) is False

    def test_partial_returns_true(self):
        assert should_autofix({"status": "partial", "failed": 1}) is True

    def test_code_error_returns_true(self):
        assert should_autofix({"status": "error", "error": "ElementHandle.click: Timeout"}) is True

    def test_vpn_error_skipped(self):
        assert should_autofix({"status": "error", "error": "VPN not connected (Remote Access Required)"}) is False

    def test_page_load_error_skipped(self):
        assert should_autofix({"status": "error", "error": "Page failed to load (VPN? SSO expired?)"}) is False

    def test_sso_error_skipped(self):
        assert should_autofix({"status": "error", "error": "SSO login timeout"}) is False

    def test_remote_access_skipped(self):
        assert should_autofix({"status": "error", "error": "Remote Access Required"}) is False

    def test_unknown_status_returns_false(self):
        assert should_autofix({"status": "unknown"}) is False

    def test_empty_dict_returns_false(self):
        assert should_autofix({}) is False

    @pytest.mark.parametrize("pattern", SKIP_AUTOFIX_PATTERNS)
    def test_all_skip_patterns_are_excluded(self, pattern):
        """Every pattern in SKIP_AUTOFIX_PATTERNS should prevent autofix."""
        assert should_autofix({"status": "error", "error": f"Something {pattern} happened"}) is False


# ── Autofix: build_autofix_prompt ────────────────────────────────────────────

class TestBuildAutofixPrompt:
    def test_prompt_contains_failure_status(self):
        prompt = build_autofix_prompt({"status": "partial", "failed": 2, "details": []})
        assert "partial" in prompt
        assert "Failed extensions: 2" in prompt

    def test_prompt_contains_error_message(self):
        prompt = build_autofix_prompt({"status": "error", "error": "Timeout 30000ms"})
        assert "Timeout 30000ms" in prompt

    def test_prompt_contains_failed_membership_details(self):
        details = [{"name": "My Group", "role": "Reader", "days_left": 5}]
        prompt = build_autofix_prompt({"status": "partial", "failed": 1, "details": details})
        assert "My Group" in prompt
        assert "Reader" in prompt

    def test_prompt_contains_instructions(self):
        prompt = build_autofix_prompt({"status": "error", "error": "test"})
        assert "renew_entitlements.py" in prompt
        assert "Playwright" in prompt
        assert "Terms & Conditions" in prompt or "Terms and Conditions" in prompt
