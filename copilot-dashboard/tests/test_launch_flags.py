"""
Unit tests for launch_copilot_resume() — verifies that flags like --autopilot
and --allow-all are correctly passed through 'wt' to the child 'copilot' process.

The critical invariant: wt must receive a '--' separator so it doesn't consume
flags meant for copilot.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import launch


def _capture_launch(**kwargs):
    """Call launch_copilot_resume with Popen mocked, return captured args."""
    defaults = dict(
        session_id="test-session-abc123",
        cwd=os.path.expanduser("~"),
        autopilot=True,
        allow_all=True,
        same_window=True,
    )
    defaults.update(kwargs)
    captured = {}

    def fake_popen(args, **kw):
        captured["args"] = list(args)
        return MagicMock()

    with patch("subprocess.Popen", side_effect=fake_popen):
        ok, msg = launch.launch_copilot_resume(**defaults)

    return ok, msg, captured.get("args", [])


class TestWtDoubleDashSeparator(unittest.TestCase):
    """The '--' separator prevents wt from consuming copilot flags."""

    def test_double_dash_present(self):
        _, _, args = _capture_launch()
        self.assertIn("--", args,
                       "wt command must include '--' to separate wt flags from child command")

    def test_copilot_after_separator(self):
        _, _, args = _capture_launch()
        dd = args.index("--")
        self.assertEqual(args[dd + 1], "copilot",
                         "'copilot' must be the first arg after '--'")

    def test_autopilot_after_separator(self):
        _, _, args = _capture_launch(autopilot=True)
        dd = args.index("--")
        child_args = args[dd + 1:]
        self.assertIn("--autopilot", child_args,
                       "--autopilot must appear in child command (after '--')")

    def test_allow_all_after_separator(self):
        _, _, args = _capture_launch(allow_all=True)
        dd = args.index("--")
        child_args = args[dd + 1:]
        self.assertIn("--allow-all", child_args,
                       "--allow-all must appear in child command (after '--')")

    def test_resume_after_separator(self):
        _, _, args = _capture_launch()
        dd = args.index("--")
        child_args = args[dd + 1:]
        resume_args = [a for a in child_args if a.startswith("--resume=")]
        self.assertTrue(len(resume_args) == 1,
                        "--resume=<id> must appear in child command (after '--')")


class TestAutopilotFlag(unittest.TestCase):

    def test_autopilot_on(self):
        _, _, args = _capture_launch(autopilot=True)
        self.assertIn("--autopilot", args)

    def test_autopilot_off(self):
        _, _, args = _capture_launch(autopilot=False)
        self.assertNotIn("--autopilot", args)

    def test_autopilot_default_true(self):
        """Default is autopilot=True."""
        captured = {}

        def fake_popen(a, **kw):
            captured["args"] = list(a)
            return MagicMock()

        with patch("subprocess.Popen", side_effect=fake_popen):
            launch.launch_copilot_resume("sid", os.path.expanduser("~"))

        self.assertIn("--autopilot", captured["args"])


class TestAllowAllFlag(unittest.TestCase):

    def test_allow_all_on(self):
        _, _, args = _capture_launch(allow_all=True)
        self.assertIn("--allow-all", args)

    def test_allow_all_off(self):
        _, _, args = _capture_launch(allow_all=False)
        self.assertNotIn("--allow-all", args)

    def test_no_legacy_flag(self):
        """Must use --allow-all, NOT --allowAllPermissions."""
        _, _, args = _capture_launch(allow_all=True)
        joined = " ".join(args)
        self.assertNotIn("allowAllPermissions", joined)


class TestSameWindowFlag(unittest.TestCase):

    def test_same_window_on(self):
        _, _, args = _capture_launch(same_window=True)
        self.assertIn("-w", args)
        self.assertEqual(args[args.index("-w") + 1], "0")

    def test_same_window_off(self):
        _, _, args = _capture_launch(same_window=False)
        self.assertNotIn("-w", args)


class TestPromptPassthrough(unittest.TestCase):

    def test_prompt_passed_as_message(self):
        _, _, args = _capture_launch(prompt="do something")
        self.assertIn("--message", args)
        self.assertIn("do something", args)

    def test_no_prompt_no_message_flag(self):
        _, _, args = _capture_launch(prompt=None)
        self.assertNotIn("--message", args)


if __name__ == "__main__":
    unittest.main()
