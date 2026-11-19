"""
Practical 8: Python Implementation of GraphX PageRank
Reads data/vertices.csv and data/edges.csv and applies PageRank algorithm
matching Apache Spark GraphX graph.pageRank(0.0001).
"""
import csv
import os

def load_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    vertices_path = os.path.join(base_dir, "data", "vertices.csv")
    edges_path = os.path.join(base_dir, "data", "edges.csv")

    users = {}
    with open(vertices_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if row:
                users[int(row[0])] = row[1]

    edges = []
    with open(edges_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if row:
                edges.append((int(row[0]), int(row[1])))

    return users, edges


def compute_pagerank(users, edges, tol=0.0001, reset_prob=0.15):
    nodes = list(users.keys())
    out_deg = {n: 0 for n in nodes}
    in_edges = {n: [] for n in nodes}

    for u, v in edges:
        out_deg[u] += 1
        in_edges[v].append(u)

    # Initial PageRank in GraphX dynamic mode
    ranks = {n: reset_prob for n in nodes}

    prev_ranks = {n: 0.0 for n in nodes}
    max_diff = 1.0

    while max_diff > tol:
        new_ranks = {}
        for n in nodes:
            incoming_sum = sum(ranks[u] / out_deg[u] for u in in_edges[n] if out_deg[u] > 0)
            new_ranks[n] = reset_prob + (1.0 - reset_prob) * incoming_sum

        max_diff = max(abs(new_ranks[n] - ranks[n]) for n in nodes)
        ranks = new_ranks

    return ranks


if __name__ == "__main__":
    print("Spark Started Successfully")
    users, edges = load_data()
    print("Graph Created Successfully")
    print("Total Users:", len(users))
    print("Total Relationships:", len(edges))

    ranks = compute_pagerank(users, edges)

    # Combine names and ranks, sorted descending
    results = sorted(
        [(users[uid], score) for uid, score in ranks.items()],
        key=lambda x: x[1],
        reverse=True
    )

    print("\nPageRank Results")
    for name, score in results:
        print(f"{name} : {score}")

    print("\nMost Influential User")
    print(f"({results[0][0]},{results[0][1]})")
