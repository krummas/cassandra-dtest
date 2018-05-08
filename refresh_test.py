import time

from dtest import Tester
from ccmlib.node import ToolError
import pytest
import os.path
import shutil
import tempfile
import os

since = pytest.mark.since

class TestRefresh(Tester):
    @since('3.0')
    def test_refresh_deadlock_startup(self):
        """ Test refresh deadlock during startup (CASSANDRA-14310) """
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.byteman_port = '8100'
        node.import_config_files()
        self.cluster.start(wait_other_notice=True)
        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.a (id int primary key, d text)")
        session.execute("CREATE TABLE ks.b (id int primary key, d text)")
        node.nodetool("disableautocompaction") # make sure we have more than 1 sstable
        for x in range(0, 10):
            session.execute("INSERT INTO ks.a (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            session.execute("INSERT INTO ks.b (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            node.flush()
        node.stop()
        node.update_startup_byteman_script('byteman/sstable_open_delay.btm')
        node.start()
        node.watch_log_for("opening keyspace ks", filename="debug.log")
        time.sleep(5)
        for x in range(0, 20):
            try:
                node.nodetool("refresh ks a")
                node.nodetool("refresh ks b")
            except ToolError:
                pass # this is OK post-14310 - we just don't want to hang forever
            time.sleep(1)

    def test_import_many_directories(self):
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.start(wait_for_binary_proto=True)
        self.prepare_ks(node)
        dirs = self.copy_sstables(node, 'ks', 'a', False)
        dirs.extend(self.copy_sstables(node, 'ks', 'a', False))
        dirs.extend(self.copy_sstables(node, 'ks', 'a', False))
        self.run_import(node, 'ks', 'a', dirs)

    def test_verify_data_imported(self):
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.start(wait_for_binary_proto=True)
        self.prepare_ks(node)
        node.stop()
        dirs = self.copy_sstables(node, 'ks', 'a', True)
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        for x in range(100, 110):
            session.execute("INSERT INTO ks.a (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            node.flush()
        node.stop()
        dirs.extend(self.copy_sstables(node, 'ks', 'a', True))
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        assert len(list(session.execute("select * from ks.a"))) == 0
        self.run_import(node, 'ks', 'a', dirs)
        res = session.execute("select * from ks.a")
        assert len(list(res)) == 20
        for row in res:
            id = int(row['id'])
            assert (id >=0 and id < 10) or (id >= 100 and id < 110)

    def test_verify_data_imported_corrupt(self):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + ['Failed verifying sstable']
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.start(wait_for_binary_proto=True)
        self.prepare_ks(node)
        node.stop()
        dirs = self.copy_sstables(node, 'ks', 'a', True)
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        for x in range(100, 110):
            session.execute("INSERT INTO ks.a (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            node.flush()
        node.stop()
        dirs.extend(self.copy_sstables(node, 'ks', 'a', True))
        node.start(wait_for_binary_proto=True)
        dirs1_before = os.listdir(dirs[1])
        for f in dirs1_before:
            if '-Digest' in f:
                self.corrupt_file(os.path.join(dirs[1], f))
                break

        session = self.patient_cql_connection(node)
        assert len(list(session.execute("select * from ks.a"))) == 0
        err = self.run_import(node, 'ks', 'a', dirs)
        assert dirs[1] in err

        res = session.execute("select * from ks.a")
        assert len(list(res)) == 10
        # verify that no data from the corrupt dir was imported:
        for row in res:
            id = int(row['id'])
            assert (id >=0 and id < 10)

    def test_import_corrupt(self):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + ['Failed verifying sstable']
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.start(wait_for_binary_proto=True)
        self.prepare_ks(node)
        node.stop()
        dirs = self.copy_sstables(node, 'ks', 'a', False)
        dirs.extend(self.copy_sstables(node, 'ks' ,'a', False))
        dirs.extend(self.copy_sstables(node, 'ks' ,'a', False))
        dirs1_before = os.listdir(dirs[1])
        for f in dirs1_before:
            if '-Digest' in f:
                self.corrupt_file(os.path.join(dirs[1], f))
                break
        node.start(wait_for_binary_proto=True)
        err = self.run_import(node, 'ks', 'a', dirs)
        assert dirs[1] in err
        # sstables should be moved back if directory failed
        assert dirs1_before == os.listdir(dirs[1])
        # imported sstables are moved to datadirs
        assert len(os.listdir(dirs[0])) == 0
        assert len(os.listdir(dirs[2])) == 0

    def test_import_corrupt_noverify(self):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + ['Failed verifying sstable', 'Failed importing sstables in directory']
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        node.start(wait_for_binary_proto=True)
        self.prepare_ks(node)
        node.stop()
        dirs = self.copy_sstables(node, 'ks', 'a', False)
        dirs.extend(self.copy_sstables(node, 'ks' ,'a', False))
        dirs.extend(self.copy_sstables(node, 'ks' ,'a', False))
        dirs1_before = os.listdir(dirs[1])
        for f in dirs1_before:
            if '-Statistics' in f:
                self.corrupt_file(os.path.join(dirs[1], f))
                break
        node.start(wait_for_binary_proto=True)
        # this will fail after we have started moving sstables to the data dirs, forcing cassandra to move them back
        err = self.run_import(node, 'ks', 'a', dirs, options='--quick')
        assert dirs[1] in err
        # sstables should be moved back if directory failed
        assert dirs1_before == os.listdir(dirs[1])
        # imported sstables are moved to datadirs
        assert len(os.listdir(dirs[0])) == 0
        assert len(os.listdir(dirs[2])) == 0

    def corrupt_file(self, f_to_corrupt):
        with open(f_to_corrupt, 'wb') as f:
            f.write(b"abc")

    def prepare_ks(self, node):
        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.a (id int primary key, d text)")
        node.nodetool("disableautocompaction") # make sure we have more than 1 sstable
        for x in range(0, 10):
            session.execute("INSERT INTO ks.a (id, d) VALUES (%d, '%d %d')"%(x, x, x))
            node.flush()

    def copy_sstables(self, node, ks, cf, move):
        tempdir = tempfile.mkdtemp()
        sstable_dirs = []
        for sstabledir in node.get_sstables_per_data_directory(ks, cf):
            directory_to_copy = os.path.dirname(sstabledir[0])
            cf_dir = os.path.basename(directory_to_copy)
            ks_dir = os.path.basename(os.path.dirname(directory_to_copy))
            target = os.path.join(tempdir, ks_dir, cf_dir)
            if not os.path.exists(target):
                os.makedirs(target)
                sstable_dirs.append(target)
            for f in os.listdir(directory_to_copy):
                if f == 'backups':
                    continue
                target_file = os.path.join(target, os.path.basename(f))
                src_file = os.path.join(directory_to_copy, f)
                if move:
                    shutil.move(src_file, target_file)
                else:
                    shutil.copy(src_file, target_file)
        return sstable_dirs

    def run_import(self, node, ks, cf, dirs, **kwargs):
        try:
            options = kwargs['options'] if 'options' in kwargs else ''
            cmd = 'import %s %s %s %s'%(options, ks, cf, " ".join(dirs))
            print(cmd)
            res = node.nodetool(cmd)
        except ToolError as e:
            return e.stderr
        return None

