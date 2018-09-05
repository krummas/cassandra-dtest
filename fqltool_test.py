import pytest
import logging
import os
import subprocess
import tempfile

from dtest import Tester
from tools.data import rows_to_list
from shutil import rmtree

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('4.0')
class TestFQLTool(Tester):
    def test_replay(self):
        self.cluster.populate(2).start(wait_for_binary_proto=True)
        node1, node2 = self.cluster.nodelist()
        tmpdir = tempfile.mkdtemp()
        tmpdir2 = tempfile.mkdtemp()
        node1.nodetool("enablefullquerylog --path=%s"%tmpdir)
        node2.nodetool("enablefullquerylog --path=%s"%tmpdir2)
        node1.stress(['write', 'n=1000'])
        node1.flush()
        node2.flush()
        node1.nodetool("disablefullquerylog")
        node2.nodetool("disablefullquerylog")
        node1.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)
        for d in node1.data_directories():
            rmtree(d)
            os.mkdir(d)
        for d in node2.data_directories():
            rmtree(d)
            os.mkdir(d)

        node1.start(wait_for_binary_proto=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        # make sure the node is empty:
        got_exception = False
        try:
            node1.stress(['read', 'n=1000'])
        except:
            got_exception = True
        assert got_exception
        # replay the log files
        self._run_fqltool_replay(node1, [tmpdir, tmpdir2], "127.0.0.1", None, None)
        # and verify the data is there
        node1.stress(['read', 'n=1000'])

    def test_compare(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        fqldir = tempfile.mkdtemp()

        node1.stress(['write', 'n=1000'])
        node1.flush()
        node1.nodetool("enablefullquerylog --path=%s"%fqldir)
        node1.stress(['read', 'n=1000'])
        node1.nodetool("disablefullquerylog")

        results1 = tempfile.mkdtemp()
        queries1 = tempfile.mkdtemp()
        self._run_fqltool_replay(node1, [fqldir], "127.0.0.1", queries1, results1)

        results2 = tempfile.mkdtemp()
        queries2 = tempfile.mkdtemp()
        self._run_fqltool_replay(node1, [fqldir], "127.0.0.1", queries2, results2)

        output = self._run_fqltool_compare(node1, queries1, [results1, results2])
        assert "MISMATCH" not in output # running the same reads against the same data

    def test_compare_mismatch(self):
        """
        generates two fql log files with different data (seq is different when running stress)
        then asserts that the replays of each generates a mismatch
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]

        fqldir1 = tempfile.mkdtemp()
        node1.nodetool("enablefullquerylog --path=%s"%fqldir1)
        node1.stress(['write', 'n=1000'])
        node1.flush()
        node1.stress(['read', 'n=1000'])
        node1.nodetool("disablefullquerylog")

        node1.stop()
        for d in node1.data_directories():
            rmtree(d)
            os.mkdir(d)
        node1.start(wait_for_binary_proto=True)

        fqldir2 = tempfile.mkdtemp()
        node1.nodetool("enablefullquerylog --path=%s"%fqldir2)
        node1.stress(['write', 'n=1000', '-pop','seq=1000..2000'])
        node1.flush()
        node1.stress(['read', 'n=1000','-pop','seq=1000..2000'])
        node1.nodetool("disablefullquerylog")
        node1.stop()
        for d in node1.data_directories():
            rmtree(d)
            os.mkdir(d)
        node1.start(wait_for_binary_proto=True)

        results1 = tempfile.mkdtemp()
        queries1 = tempfile.mkdtemp()
        self._run_fqltool_replay(node1, [fqldir1], "127.0.0.1", queries1, results1)
        node1.stop()
        for d in node1.data_directories():
            rmtree(d)
            os.mkdir(d)
        node1.start(wait_for_binary_proto=True)
        results2 = tempfile.mkdtemp()
        queries2 = tempfile.mkdtemp()
        self._run_fqltool_replay(node1, [fqldir2], "127.0.0.1", queries2, results2)

        output = self._run_fqltool_compare(node1, queries1, [results1, results2])
        assert "MISMATCH" in output # running the same reads against the same data

    def _run_fqltool_replay(self, node, logdirs, target, queries, results):
        fqltool = self.fqltool(node)
        args = [fqltool, "replay", "--target %s"%target]
        if queries is not None:
            args.append("--store-queries %s"%queries)
        if results is not None:
            args.append("--results %s"%results)
        args.extend(logdirs)
        rc = subprocess.call(args)
        assert rc == 0

    def _run_fqltool_compare(self, node, queries, results):
        fqltool = self.fqltool(node)
        args = [fqltool, "compare", "--queries %s"%queries]
        args.extend([os.path.join(r, "127.0.0.1") for r in results])
        logger.info(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        (stdout, stderr) = p.communicate()
        logger.info(stdout)
        return stdout

    def fqltool(self, node):
        cdir = node.get_install_dir()
        fqltool = os.path.join(cdir, 'tools', 'bin', 'fqltool')
        return fqltool
