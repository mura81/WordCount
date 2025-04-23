import grpc
import logging
import word_count_pb2 as word_count_pb2
import word_count_pb2_grpc as word_count_pb2_grpc

class SidecarProxy:
    def __init__(self, node_id, role):
        self.node_id = node_id
        self.role = role
        self.logger = logging.getLogger(f"Sidecar_{node_id}_{role}")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(f"logs/sidecar_{node_id}_{role}.log")
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

    def broadcast_node_counts(self, target, message):
        self._send_message(target, "BroadcastNodeCounts", message)

    def assign_range(self, target, start_letter, end_letter, proposer_id):
        message = word_count_pb2.LetterRange(start_letter=start_letter, end_letter=end_letter, proposer_id=proposer_id)
        self._send_message(target, "AssignRange", message)

    def multicast_line(self, targets, content):
        message = word_count_pb2.DocumentLine(content=content)
        for target in targets:
            self._send_message(target, "MulticastLine", message)

    def submit_counts(self, targets, proposer_id, counts):
        message = word_count_pb2.WordCounts(proposer_id=proposer_id)
        for letter, data in counts.items():
            word_count = word_count_pb2.WordCount(count=data["count"], words=data["words"])
            message.counts[letter].CopyFrom(word_count)
        for target in targets:
            self._send_message(target, "SubmitCounts", message)

    def submit_validated_counts(self, target, proposer_id, counts):
        message = word_count_pb2.WordCounts(proposer_id=proposer_id)
        for letter, data in counts.items():
            word_count = word_count_pb2.WordCount(count=data["count"], words=data["words"])
            message.counts[letter].CopyFrom(word_count)
        self._send_message(target, "SubmitValidatedCounts", message)

    def end_document(self, target):
        message = word_count_pb2.Empty()
        self._send_message(target, "EndDocument", message)

    def register_node(self, target, role, address):
        message = word_count_pb2.NodeInfo(role=role, address=address)
        self._send_message(target, "RegisterNode", message)

    def _send_message(self, target, method, message):
        self.logger.info(f"Sending {method} to {target}")
        try:
            with grpc.insecure_channel(target) as channel:
                stub = word_count_pb2_grpc.DistributedWordCountStub(channel)
                getattr(stub, method)(message)
            self.logger.info(f"Successfully sent {method} to {target}")
        except Exception as e:
            self.logger.error(f"Failed to send {method} to {target}: {e}")