import logging
import re
import pytest

from dtest import Tester
from tools.misc import ImmutableMapping

since = pytest.mark.since
logger = logging.getLogger(__name__)

@since('4')
class TestOptimiseStreams(Tester):
    """
    Tests for optimised streams - make sure that we only stream from the correct nodes
    """
    def test_basic(self):
        """
        3 node cluster, write data, take a node down, write more data and repair
        Make sure that the down node only streams from one of the up ones
        """
        cluster = self.cluster
        logger.debug('Starting nodes')
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false'})
        self.fixture_dtest_setup.init_default_config()
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        logger.debug('running stress')
        node1.stress(stress_options=['write', 'n=50K', 'no-warmup', 'cl=ALL',
                                     '-schema', 'replication(factor=3)', '-rate', 'threads=50'])
        logger.debug('stopping node2')
        node2.stop(wait_other_notice=True)
        node1.stress(stress_options=['write', 'n=20', 'no-warmup', 'cl=EACH_QUORUM',
                                     '-schema', 'replication(factor=3)', '-rate', 'threads=50'])
        logger.debug('starting node2')
        node2.start(wait_other_notice=True)
        logger.debug('running repair')
        node1.repair(['keyspace1', 'standard1', '-os'])
        node1_receiving = self._get_receiving(node1)
        assert {node2.address_and_port()} == node1_receiving
        node3_receiving = self._get_receiving(node3)
        assert {node2.address_and_port()} == node3_receiving

        fetching_ranges = self._get_fetching_ranges(node1)
        # 127.0.0.2 should not fetch the same ranges from 127.0.0.3 and 127.0.0.1:
        node2_from_node1_key = "%s<-%s"%(node2.address_and_port(), node1.address_and_port())
        node2_from_node3_key = "%s<-%s"%(node2.address_and_port(), node3.address_and_port())
        for fetching_range in fetching_ranges[node2_from_node3_key]:
            assert fetching_range not in fetching_ranges[node2_from_node1_key]
        self._assert_everything_repaired()
        self._ensure_in_sync(node1)

    def test_multidc(self):
        """
        start a 3 node 2dc cluster
        stop a node in dc1
        write data
        repair
        make sure that the down node only streams from the local dc.
        """
        cluster = self.cluster
        logger.debug('Starting nodes')
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate([3, 3]).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]
        node3 = cluster.nodelist()[2]

        logger.debug('running stress')
        node1.stress(stress_options=['write', 'n=50K', 'no-warmup', 'cl=ONE',
                                     '-schema',
                                     'replication(strategy=NetworkTopologyStrategy,dc1=3,dc2=3)',
                                     '-rate', 'threads=50'])
        node2.stop(wait_other_notice=True)
        node1.stress(stress_options=['write', 'n=20', 'no-warmup', 'cl=ONE',
                                     '-rate', 'threads=50'])
        logger.debug('starting node2')
        node2.start(wait_other_notice=True)
        logger.debug('running repair')
        node1.repair(['keyspace1', 'standard1', '-os'])
        fetching_ranges = self._get_fetching_ranges(node1)
        all_ips = [node.address_and_port() for node in cluster.nodelist()]
        # all nodes should fetch from node2:
        incoming_to_node2 = 0
        node1_ip = node1.address_and_port()
        node2_ip = node2.address_and_port()
        node3_ip = node3.address_and_port()
        for ip in all_ips:
            if ip != node2_ip:
                key = '%s<-%s' % (ip, node2_ip)
                assert key in fetching_ranges
                assert len(fetching_ranges[key]) > 0
            key = '%s<-%s' % (node2_ip, ip)
            if key in fetching_ranges:
                incoming_to_node2 += 1

        # make sure we only stream in from the local dc
        to_n2_from_n1 = "%s<-%s" % (node2_ip, node1_ip)
        to_n2_from_n3 = "%s<-%s" % (node2_ip, node3_ip)
        assert to_n2_from_n1 in fetching_ranges
        assert to_n2_from_n3 in fetching_ranges

        # make sure we dont stream duplicate ranges from n1 and n3 to n2
        for fetching_range in fetching_ranges[to_n2_from_n1]:
            assert fetching_range not in fetching_ranges[to_n2_from_n3]

        assert 2 == incoming_to_node2
        self._ensure_in_sync(node1)
        self._assert_everything_repaired()

    def _get_receiving(self, node):
        rexp = r'Start receiving file \#\d+ from ([^,]+),'
        res = node.grep_log(rexp, filename='debug.log')
        receiving_from = set()
        for line, m in res:
            receiving_from.add(m.group(1))
        return receiving_from

    def _get_fetching_ranges(self, node):
        rexp = r'([^\s]+) is about to fetch \[(.*)\] from (.*)'
        res = node.grep_log(rexp, filename='debug.log')
        fetching = dict()
        for line, m in res:
            key = '%s<-%s' % (m.group(1), m.group(3))
            ranges = []
            for l in m.group(2).split(", "):
                ranges.append(re.sub('.*\s([^\s]+)\s.*', '\\1', l))
            fetching[key] = ranges
        return fetching

    def _ensure_in_sync(self, node):
        assert "Repaired data is in sync" in node.repair(['-vd', 'keyspace1', 'standard1']).stdout

    def _assert_everything_repaired(self):
        repairedat_re = re.compile(r'^Repaired at: (\d+).*', re.MULTILINE)
        for node in self.cluster.nodelist():
            metadata = node.run_sstablemetadata(keyspace='keyspace1').stdout
            match = re.search(repairedat_re, metadata)
            repaired_at = int(match.group(1))
            assert repaired_at > 0
