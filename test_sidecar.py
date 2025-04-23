# test_sidecar.py

from sidecar import SidecarProxy

# Test configuration
test_target = "localhost:50052"  # This should match the port of a running node
proxy = SidecarProxy(node_id="test", role="coordinator")

# 1. Test broadcast_node_counts
proxy.broadcast_node_counts(
    targets=[test_target],
    proposers=2,
    acceptors=2,
    learners=1
)

# 2. Test assign_range
proxy.assign_range(
    target=test_target,
    start_letter="a",
    end_letter="f",
    proposer_id="proposer1"
)

# 3. Test multicast_line
proxy.multicast_line(
    targets=[test_target],
    content="This is a line from a document."
)

# 4. Test submit_counts
proxy.submit_counts(
    targets=[test_target],
    proposer_id="proposer1",
    counts={
        "a": {"count": 2, "words": ["apple", "ant"]},
        "b": {"count": 1, "words": ["banana"]}
    }
)

# 5. Test submit_validated_counts
proxy.submit_validated_counts(
    target=test_target,
    proposer_id="proposer1",
    counts={
        "a": {"count": 2, "words": ["apple", "ant"]},
        "b": {"count": 1, "words": ["banana"]}
    }
)

print("✅ Sidecar tests finished! Check logs for details.")
