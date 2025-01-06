import matplotlib.pyplot as plt
import matplotlib.animation as animation
import networkx as nx
import re
import time
import glob
import json
from collections import defaultdict
import colorsys
import os
import numpy as np

class DagVisualizer:
    def __init__(self, test_dir):
        self.test_dir = test_dir
        self.G = nx.DiGraph()
        self.pos = {}
        self.current_round = 0
        self.certificates_by_round = defaultdict(list)
        self.validator_colors = {}  # Map of validator ID to color
        self.last_read_timestamp = 0
        self.visible_rounds = 5  # Number of rounds to show
        
        # Setup the plot
        plt.style.use('dark_background')
        self.fig, self.ax = plt.subplots(figsize=(16, 10))
        self.fig.patch.set_facecolor('#1C1C1C')
        self.ax.set_facecolor('#1C1C1C')
        
    def get_validator_color(self, validator_id):
        if validator_id not in self.validator_colors:
            # Generate a new color using golden ratio for good distribution
            hue = len(self.validator_colors) * 0.618033988749895
            hue = hue - int(hue)
            self.validator_colors[validator_id] = colorsys.hsv_to_rgb(hue, 0.8, 0.95)
        return self.validator_colors[validator_id]
        
    def read_dag_state(self):
        # Look for output.dag files in validator directories
        output_files = glob.glob(os.path.join(self.test_dir, "validator_*/primary/output.dag"))
        latest_state = None
        latest_timestamp = self.last_read_timestamp

        for output_file in output_files:
            try:
                with open(output_file, 'r') as f:
                    lines = f.readlines()
                    
                    # Process only new lines based on timestamp
                    for line in lines:
                        try:
                            state = json.loads(line.strip())
                            if state['timestamp'] > latest_timestamp:
                                latest_timestamp = state['timestamp']
                                latest_state = state
                        except json.JSONDecodeError:
                            continue
                            
            except FileNotFoundError:
                continue

        if latest_state:
            self.last_read_timestamp = latest_timestamp
            self.current_round = latest_state['current_round']
            
            # Update graph
            self.G.clear()
            
            # Filter vertices to only include the last N rounds
            min_round = max(0, self.current_round - self.visible_rounds + 1)
            filtered_vertices = [v for v in latest_state['vertices'] 
                               if v['round'] >= min_round]
            
            # Add vertices
            for vertex in filtered_vertices:
                self.G.add_node(vertex['id'], 
                              round=vertex['round'],
                              author=vertex['author'])
                
            # Add edges between visible vertices
            visible_nodes = set(self.G.nodes())
            for edge in latest_state['edges']:
                if edge['from'] in visible_nodes and edge['to'] in visible_nodes:
                    self.G.add_edge(edge['from'], edge['to'])
            
            # Update certificates by round
            self.certificates_by_round.clear()
            for vertex in filtered_vertices:
                self.certificates_by_round[vertex['round']].append(vertex['id'])
    
    def update_layout(self):
        # Position nodes by round level, from bottom to top
        spacing_x = 3.0  # Increased horizontal spacing between nodes
        spacing_y = 1.5  # Vertical spacing between rounds
        
        # Get min and max rounds for scaling
        min_round = min(self.certificates_by_round.keys()) if self.certificates_by_round else 0
        max_round = max(self.certificates_by_round.keys()) if self.certificates_by_round else 0
        
        for round_num, certs in self.certificates_by_round.items():
            # Normalize y position to be between 0 and 1
            y = (round_num - min_round) * spacing_y
            
            # Sort certificates by author to group them by validator
            sorted_certs = sorted(certs, key=lambda c: self.G.nodes[c]['author'])
            
            for i, cert_id in enumerate(sorted_certs):
                # Center certificates horizontally with consistent spacing
                x = (i - (len(certs) - 1) / 2) * spacing_x
                self.pos[cert_id] = (x, y)
    
    def update(self, frame):
        self.ax.clear()
        self.read_dag_state()
        self.update_layout()
        
        if not self.G.nodes():
            self.ax.text(0.5, 0.5, 'Waiting for certificates...', 
                        ha='center', va='center', transform=self.ax.transAxes,
                        color='white', fontsize=12)
            return
        
        # Draw edges first with curved arrows
        nx.draw_networkx_edges(self.G, pos=self.pos, ax=self.ax,
                             edge_color='#404040', arrows=True,
                             arrowsize=20, arrowstyle='->',
                             connectionstyle='arc3,rad=0.2',
                             alpha=0.3)
        
        # Draw nodes and labels
        for node in self.G.nodes():
            author = self.G.nodes[node]['author']
            color = self.get_validator_color(author)
            round_num = self.G.nodes[node]['round']
            
            # Draw node
            nx.draw_networkx_nodes(self.G, pos=self.pos,
                                 nodelist=[node],
                                 node_color=[color],
                                 node_size=1500,
                                 alpha=0.9,
                                 edgecolors='white',
                                 linewidths=1,
                                 ax=self.ax)
            
            # Draw label
            pos_node = self.pos[node]
            plt.text(pos_node[0], pos_node[1], f"{node[:6]}\nR{round_num}",
                    horizontalalignment='center',
                    verticalalignment='center',
                    fontsize=8,
                    color='white',
                    fontweight='bold')
        
        # Add title with current round
        self.ax.set_title(f'DAG Visualization - Round {self.current_round}',
                         color='white', pad=20, fontsize=14)
        
        # Add legend for validators
        legend_elements = [plt.Line2D([0], [0], marker='o', color='w',
                                    markerfacecolor=color, markersize=10,
                                    label=f'Validator {i+1}')
                         for i, color in enumerate(self.validator_colors.values())]
        self.ax.legend(handles=legend_elements, loc='center left',
                      bbox_to_anchor=(1, 0.5))
        
        # Set axis properties
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.ax.spines['top'].set_visible(False)
        self.ax.spines['right'].set_visible(False)
        self.ax.spines['bottom'].set_visible(False)
        self.ax.spines['left'].set_visible(False)
        
        # Set fixed axis limits with some padding
        all_pos = np.array(list(self.pos.values()))
        if len(all_pos) > 0:
            x_min, y_min = all_pos.min(axis=0) - 2
            x_max, y_max = all_pos.max(axis=0) + 2
            self.ax.set_xlim(x_min, x_max)
            self.ax.set_ylim(y_min, y_max)
        
        # Adjust layout to prevent clipping
        plt.tight_layout()
    
    def animate(self):
        ani = animation.FuncAnimation(self.fig, self.update, interval=1000)
        plt.show()

if __name__ == "__main__":
    import sys
    test_dir = sys.argv[1] if len(sys.argv) > 1 else "test"
    visualizer = DagVisualizer(test_dir)
    visualizer.animate() 