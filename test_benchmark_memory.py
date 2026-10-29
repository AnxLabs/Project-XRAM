import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('xram_benchmark',Path(__file__).parent/'benchmarks'/'local_lan.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)

class BenchmarkMemoryTests(unittest.TestCase):
    def test_linux_memory_units_and_optional_swap(self):
        status='Name:\tpython\nVmRSS:\t123 kB\nVmHWM:\t456 kB\nVmSwap:\t7 kB\n'
        with patch.object(benchmark.os,'name','posix'), patch.object(benchmark.sys,'platform','linux'), patch.object(benchmark,'Path') as mocked_path:
            mocked_path.return_value.read_text.return_value=status
            self.assertEqual(benchmark.process_memory(),{'working_set':123*1024,'peak_working_set':456*1024,'swap_bytes':7*1024})
    def test_unavailable_proc_is_not_reported_as_zero(self):
        with patch.object(benchmark.os,'name','posix'), patch.object(benchmark.sys,'platform','linux'), patch.object(benchmark,'Path') as mocked_path:
            mocked_path.return_value.read_text.side_effect=OSError('unavailable')
            self.assertIn('unavailable',benchmark.process_memory()['note'])

if __name__=='__main__': unittest.main()
