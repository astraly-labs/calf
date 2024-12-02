import json
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pydot
from networkx.drawing.nx_pydot import graphviz_layout
import os
import logging
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="DAG visualizer")
    parser.add_argument("logs", type=str, help="path to the DAG logs folder")
    parser.add_argument("dest", type=str, help="destiation file to save the animation")
    
    args = parser.parse_args()

    dag_rounds = sorted([f for f in os.listdir(args.logs) if f.endswith(".json")])

    def update(num, dag_rounds, G, ax):
        with open(os.path.join(args.logs, dag_rounds[num]), "r") as f:
            data = json.load(f)

        for node, attributes in data.items():
            if isinstance(attributes, dict) and "parents" in attributes:
                for parent in attributes["parents"]:
                    if not G.has_edge(parent, node):
                        G.add_edge(parent, node)
            else:
                logging.error("node error")
        pos = graphviz_layout(G, prog="dot")
        ax.clear()
        nx.draw(G, pos, with_labels=True, font_size=7, node_size=2000, node_color="skyblue", font_color="black", font_weight="bold", arrowsize=25, ax=ax)
        ax.set_title(f"Round {num + 1}")

    G = nx.DiGraph()
    fig, ax = plt.subplots(figsize=(15, 20))
    ani = animation.FuncAnimation(fig, update, frames=len(dag_rounds), fargs=(dag_rounds, G, ax), interval=1000, repeat=False)
    ani.save(f"{args.dest}.gif", writer="imagemagick", fps=1)
