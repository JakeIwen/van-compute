"""Keep recovery commands anchored to reviewed source rather than caller HEAD."""
from pathlib import Path
import unittest


class ComputeRunbookTests(unittest.TestCase):
    def test_rollback_pins_baseline_and_overlays_fixed_frozen_installer(self):
        repo = Path(__file__).resolve().parents[3]
        text = (repo / 'pi/docs/compute/VAN_COMPUTE.md').read_text()
        command = next(line for line in text.splitlines()
                       if 'worktree add --detach "$ROLLBACK"' in line)
        self.assertIn('worktree add --detach "$ROLLBACK" f32ce9e &&', command)
        self.assertNotIn('"$ROLLBACK" HEAD', command)
        self.assertIn('restore --source=c69a7a6', command)
        self.assertIn('cp "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/install_van_compute_worker.py"', command)
        self.assertIn('cp "$SAFE_INSTALLER_RELEASE/app/macbook/scripts/install_van_compute_worker.zsh"', command)
        self.assertTrue(command.endswith('--dry-run'))


if __name__ == '__main__':
    unittest.main()
