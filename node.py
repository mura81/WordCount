import argparse
import logging
from concurrent import futures
import grpc
import word_count_pb2 as word_count_pb2
import word_count_pb2_grpc as word_count_pb2_grpc
from sidecar import SidecarProxy
import re
import time

class Node(word_count_pb2_grpc.DistributedWordCountServicer):
    def __init__(self, node_id, role, port, coordinator_address="localhost:50051"):
        self.node_id = node_id
        self.role = role
        self.port = port
        self.coordinator_address = coordinator_address
        self.sidecar = SidecarProxy(node_id, role)
        self.logger = logging.getLogger(f"Node_{node_id}_{role}")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(f"logs/{node_id}_{role}.log")
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        self.logger.info(f"Starting node {node_id} as {role}")

        if self.role == "coordinator":
            self.proposers = []
            self.acceptors = []
            self.learners = []
            self.letter_ranges = {}

        if self.role == "proposer":
            self.start_letter = None
            self.end_letter = None
            self.word_counts = {}
            self.acceptors = []

        if self.role == "acceptor":
            self.learners = []

        if self.role == "learner":
            self.final_counts = {}

        # Register with coordinator (except for coordinator itself)
        if self.role != "coordinator":
            for attempt in range(3):  # Retry 3 times
                try:
                    self.sidecar.register_node(self.coordinator_address, self.role, f"localhost:{self.port}")
                    self.logger.info(f"Successfully registered with coordinator at {self.coordinator_address}")
                    break
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt+1} failed to register with coordinator at {self.coordinator_address}: {e}")
                    if attempt < 2:
                        time.sleep(1)  # Wait 1 second before retrying
                    else:
                        self.logger.error(f"Failed to register with coordinator after 3 attempts. Continuing without registration.")

    def discover_nodes(self, proposer_ports, acceptor_ports, learner_ports):
        if self.role != "coordinator":
            return
        self.proposers = [f"localhost:{port}" for port in proposer_ports]
        self.acceptors = [f"localhost:{port}" for port in acceptor_ports]
        self.learners = [f"localhost:{port}" for port in learner_ports]
        self.logger.info(f"Discovered {len(self.proposers)} proposers, {len(self.acceptors)} acceptors, {len(self.learners)} learners")
        self.logger.info(f"Learner addresses: {self.learners}")
        
        self.broadcast_node_counts()

    def broadcast_node_counts(self):
        if self.role != "coordinator":
            return
        all_nodes = self.proposers + self.acceptors + self.learners
        message = word_count_pb2.NodeCounts(
            proposers=len(self.proposers),
            acceptors=len(self.acceptors),
            learners=len(self.learners),
            acceptor_addresses=self.acceptors,
            learner_addresses=self.learners
        )
        self.logger.info(f"Broadcasting NodeCounts with learner_addresses: {message.learner_addresses}")
        for node in all_nodes:
            self.sidecar.broadcast_node_counts(node, message)

    def assign_letter_ranges(self):
        if self.role != "coordinator" or not self.proposers:
            return
        alphabet = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        total_letters = len(alphabet)
        num_proposers = len(self.proposers)
        base = total_letters // num_proposers
        extra = total_letters % num_proposers
        current = 0
        self.letter_ranges = {}

        for i, proposer in enumerate(self.proposers):
            proposer_id = proposer
            letters = base + (1 if i < extra else 0)
            start_letter = alphabet[current]
            end_letter = alphabet[current + letters - 1]
            self.letter_ranges[proposer_id] = (start_letter, end_letter)
            self.sidecar.assign_range(proposer, start_letter, end_letter, proposer_id)
            self.logger.info(f"Assigned range {start_letter}-{end_letter} to {proposer_id}")
            current += letters

    def multicast_document(self, document_path):
        if self.role != "coordinator":
            return
        try:
            with open(document_path, 'r') as file:
                for line in file:
                    line = line.strip()
                    if line:
                        self.sidecar.multicast_line(self.proposers, line)
                        self.logger.info(f"Multicast line: {line}")
            for proposer in self.proposers:
                self.sidecar.end_document(proposer)
            self.logger.info("Sent EndDocument to all proposers")
        except Exception as e:
            self.logger.error(f"Failed to read document: {e}")

    def send_counts(self):
        if self.role != "proposer" or not self.acceptors:
            self.logger.warning(f"Not sending counts: acceptors={self.acceptors}")
            return
        self.logger.info(f"Sending word counts to acceptors: {self.acceptors}")
        self.logger.info(f"Counts: {self.word_counts}")
        self.sidecar.submit_counts(self.acceptors, self.node_id, self.word_counts)

    def validate_counts(self, proposer_id, counts):
        if self.role != "acceptor":
            return
        validated = {}
        self.logger.info(f"Validating counts from {proposer_id}: {counts}")
        for letter, word_count in counts.items():
            expected_count = len(word_count.words)
            if word_count.count == expected_count:
                validated[letter] = {"count": word_count.count, "words": word_count.words}
                self.logger.info(f"Validated {letter} from {proposer_id}: count={expected_count}")
            else:
                self.logger.warning(f"Validation failed for {letter} from {proposer_id}: expected {expected_count}, got {word_count.count}")
        if validated and self.learners:
            self.logger.info(f"Forwarding validated counts to learner: {self.learners[0]}")
            self.sidecar.submit_validated_counts(self.learners[0], proposer_id, validated)
        else:
            self.logger.warning(f"No validated counts to forward or no learners: {self.learners}")

    def aggregate_counts(self, proposer_id, counts):
        if self.role != "learner":
            return
        self.logger.info(f"Aggregating counts from {proposer_id}: {counts}")
        for letter, word_count in counts.items():
            if letter not in self.final_counts:
                self.final_counts[letter] = {"count": 0, "words": []}
            self.final_counts[letter]["count"] += word_count.count
            self.final_counts[letter]["words"].extend(word_count.words)
        self.output_results()

    def output_results(self):
        if self.role != "learner":
            return
        output = "Starting letter | Count | Words\n"
        output += "-" * 50 + "\n"
        for letter in sorted(self.final_counts.keys()):
            count = self.final_counts[letter]["count"]
            words = ",".join(self.final_counts[letter]["words"])
            output += f"{letter} | {count} | {words}\n"
        self.logger.info(f"Final results:\n{output}")
        with open(f"results_{self.node_id}.txt", "w") as f:
            f.write(output)

    def RegisterNode(self, request, context):
        self.logger.info(f"Received node registration: role={request.role}, address={request.address}")
        if self.role != "coordinator":
            return word_count_pb2.Empty()
        if request.role == "proposer":
            if request.address not in self.proposers:
                self.proposers.append(request.address)
                self.logger.info(f"Added proposer: {request.address}")
                self.assign_letter_ranges()
                self.broadcast_node_counts()
        elif request.role == "acceptor":
            if request.address not in self.acceptors:
                self.acceptors.append(request.address)
                self.logger.info(f"Added acceptor: {request.address}")
                self.broadcast_node_counts()
        elif request.role == "learner":
            if request.address not in self.learners:
                self.learners.append(request.address)
                self.logger.info(f"Added learner: {request.address}")
                self.broadcast_node_counts()
        return word_count_pb2.Empty()

    def BroadcastNodeCounts(self, request, context):
        self.logger.info(f"Received node counts: proposers={request.proposers}, acceptors={request.acceptors}, learners={request.learners}, learner_addresses={request.learner_addresses}")
        if self.role == "proposer":
            self.acceptors = request.acceptor_addresses
            self.logger.info(f"Updated acceptors: {self.acceptors}")
        elif self.role == "acceptor":
            self.learners = request.learner_addresses
            self.logger.info(f"Updated learners: {self.learners}")
        return word_count_pb2.Empty()

    def AssignRange(self, request, context):
        self.logger.info(f"Received range assignment: {request.start_letter}-{request.end_letter} for {request.proposer_id}")
        if self.role == "proposer" and request.proposer_id == f"localhost:{self.port}":
            self.start_letter = request.start_letter
            self.end_letter = request.end_letter
            self.word_counts = {
                chr(i): {"count": 0, "words": []}
                for i in range(ord(self.start_letter), ord(self.end_letter) + 1)
            }
        return word_count_pb2.Empty()

    def MulticastLine(self, request, context):
        self.logger.info(f"Received line: {request.content}")
        if self.role == "proposer" and self.start_letter and self.end_letter:
            words = re.findall(r'\b\w+\b', request.content.lower())
            for word in words:
                if word and self.start_letter <= word[0].upper() <= self.end_letter:
                    letter = word[0].upper()
                    self.word_counts[letter]["count"] += 1
                    self.word_counts[letter]["words"].append(word)
        return word_count_pb2.Empty()

    def SubmitCounts(self, request, context):
        self.logger.info(f"Received counts from {request.proposer_id}")
        if self.role == "acceptor":
            self.validate_counts(request.proposer_id, request.counts)
        return word_count_pb2.Empty()

    def SubmitValidatedCounts(self, request, context):
        self.logger.info(f"Received validated counts from {request.proposer_id}")
        if self.role == "learner":
            self.aggregate_counts(request.proposer_id, request.counts)
        return word_count_pb2.Empty()

    def EndDocument(self, request, context):
        self.logger.info("Received EndDocument signal")
        if self.role == "proposer":
            self.send_counts()
        return word_count_pb2.Empty()

def serve(node_id, role, port, document_path=None, proposer_ports=None, acceptor_ports=None, learner_ports=None, coordinator_address="localhost:50051"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = Node(node_id, role, port, coordinator_address)
    word_count_pb2_grpc.add_DistributedWordCountServicer_to_server(node, server)
    server.add_insecure_port(f'[::]:{port}')
    logging.info(f"Node {node_id} ({role}) starting on port {port}")
    server.start()
    logging.info(f"Node {node_id} ({role}) gRPC server listening on localhost:{port}")

    if role == "coordinator":
        node.discover_nodes(proposer_ports or [], acceptor_ports or [], learner_ports or [])
        node.assign_letter_ranges()
        if document_path:
            node.multicast_document(document_path)

    server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Word Count Node")
    parser.add_argument("--id", type=str, required=True, help="Node ID")
    parser.add_argument("--role", type=str, required=True, choices=["coordinator", "proposer", "acceptor", "learner"], help="Node role")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    parser.add_argument("--document", type=str, help="Path to document (for coordinator)")
    parser.add_argument("--proposer-ports", type=int, nargs="*", default=[], help="Ports of proposer nodes")
    parser.add_argument("--acceptor-ports", type=int, nargs="*", default=[], help="Ports of acceptor nodes")
    parser.add_argument("--learner-ports", type=int, nargs="*", default=[], help="Ports of learner nodes")
    parser.add_argument("--coordinator-address", type=str, default="localhost:50051", help="Coordinator address")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    serve(
        args.id,
        args.role,
        args.port,
        args.document,
        args.proposer_ports,
        args.acceptor_ports,
        args.learner_ports,
        args.coordinator_address
    )